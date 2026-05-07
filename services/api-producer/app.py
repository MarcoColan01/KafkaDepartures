"""

HTTP -> Kafka gateway. Receives normalized FlightEvent payloads via
POST /flight and publishes them to the flight.telemetry Kafka topic.

"""

import os 
import time 
import logging
from datetime import datetime 
from typing import Any, Optional, Literal
import threading


from fastapi import FastAPI, HTTPException 
from pydantic import BaseModel, Field
from confluent_kafka import Producer, KafkaError, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC = os.getenv("TOPIC_EVENTS", "flight.telemetry")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api-producer")

app = FastAPI(title="Flight Flow API Producer")

Airport = Literal["AMS", "HEL", "OSL"]
FlightStatus = Literal[
    "SCHEDULED", "BOARDING", "LAST_CALL", "GATE_OPEN", "GATE_CLOSED",
    "DEPARTED", "DELAYED", "CANCELLED", "DIVERTED", "UNKNOWN",
]
ServiceType = Literal["J", "C", "G", "H"]
EventType = Literal["UPSERT", "DELETE"]

class FlightEvent(BaseModel):
    eventId: str = Field(default_factory=lambda: f"evt-{int(time.time()*1000)}")
    ts: float = Field(default_factory=time.time)

    
    airport: Airport
    flight_code: str
    airline_iata: str
    airline_name: str
    observed_at_utc: Optional[str] = None
    event_type: EventType = "UPSERT"
    scheduled_departure: datetime
    estimated_departure: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    delay_minutes: Optional[int] = None
    changed_fields: dict[str, dict[str, Any]] = Field(default_factory=dict)


    gate: Optional[str] = None
    terminal: Optional[str] = None
    destination_iata: str = ""
    destination_name: str = ""

    status: FlightStatus = "SCHEDULED"

    aircraft_type: Optional[str] = None

    is_codeshare: bool = False
    is_cargo: bool = False
    service_type: ServiceType = "J"

def kafka_ssl_base() -> dict:
    cfg = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION"),
        "ssl.certificate.location": os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION"),
        "ssl.key.location": os.getenv("KAFKA_SSL_KEY_LOCATION"),
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 1,
        "retries": 2147483647,
        "message.timeout.ms": 120000,
        "request.timeout.ms": 40000,
        "compression.type": "gzip",
        "client.id": "flight-api-producer",
    }
    return cfg

class ResilientProducer:
    def __init__(self, config: dict):
        self._config = config
        self._lock = threading.Lock()
        self._producer = Producer(config)

    def _reinit(self):
        log.warning("Producer in fatal state, recreating it")
        try:
            self._producer.flush(timeout=2)
        except Exception:
            pass
        self._producer = Producer(self._config)
        log.info("Producer recreated")

    def produce(self, *args, **kwargs):
        with self._lock:
            try:
                self._producer.produce(*args, **kwargs)
            except KafkaException as e:
                # Detect fatal state: re-init and retry once
                err = e.args[0] if e.args else None
                fatal = err is not None and (
                    getattr(err, "fatal", lambda: False)()
                    or err.code() == KafkaError._FATAL
                )
                if fatal:
                    self._reinit()
                    self._producer.produce(*args, **kwargs)
                else:
                    raise

    def poll(self, timeout):
        with self._lock:
            try:
                self._producer.poll(timeout)
            except KafkaException as e:
                err = e.args[0] if e.args else None
                fatal = err is not None and (
                    getattr(err, "fatal", lambda: False)()
                    or err.code() == KafkaError._FATAL
                )
                if fatal:
                    self._reinit()

    def flush(self, timeout):
        with self._lock:
            try:
                return self._producer.flush(timeout)
            except KafkaException:
                self._reinit()
                return 0


producer = ResilientProducer(kafka_ssl_base())

def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        log.info("delivered key=%s to %s[%d]@%d",
        msg.key().decode() if msg.key() else None,
        msg.topic(),
        msg.partition(),
        msg.offset(),
        )

@app.get("/healthcheck")
def healthcheck():
    return {"ok":True, "status": "Flight API Producer running..."}


@app.post("/flight")
def produce_flight(data:FlightEvent):
    key = f"{data.airport}:{data.flight_code}"
    payload = data.model_dump_json()

    try:
        producer.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)

        return {
            "status": "queued",
            "eventId": data.eventId,
            "key": key,
            "topic": TOPIC,
        }
    except BufferError:
        raise HTTPException(status_code=503, detail="Producer queue full")
    except KafkaError as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown():
    log.info("Flushing producer before shutdown")
    producer.flush(timeout=10)