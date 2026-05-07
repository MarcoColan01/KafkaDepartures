import os
import json
import logging
import signal
import time
from datetime import datetime, timezone, time as dtime
import airportsdata

from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_IN = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_OUT = os.getenv("TOPIC_ALERTS", "flight.alerts")
GROUP_ID = os.getenv("GROUP_ID", "notifier-group")

if not GROUP_ID or not GROUP_ID.strip():
    raise RuntimeError("GROUP_ID env var is empty or not set.")



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("notifier")

IATA_AIRPORTS = airportsdata.load("IATA")
log.info("airportsdata loaded: %d entries", len(IATA_AIRPORTS))

def kafka_ssl_base() -> dict:
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION", "/app/security/ca.crt"),
        "ssl.certificate.location": os.getenv(
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "/app/security/client-creds/kafka.client.certificate.pem",
        ),
        "ssl.key.location": os.getenv(
            "KAFKA_SSL_KEY_LOCATION",
            "/app/security/client-creds/kafka.client.key",
        ),
    }


def seek_to_start_of_today_utc(consumer: Consumer, topic: str, timeout: float = 15.0) -> None:
    """Force the consumer to read from 00:00 UTC of the current day."""
    log.info("Waiting for partition assignment on %s ...", topic)
    deadline = time.time() + timeout
    while not consumer.assignment() and time.time() < deadline:
        consumer.poll(0.5)

    parts = consumer.assignment()
    if not parts:
        log.warning("No partitions assigned within %.1fs; skipping seek", timeout)
        return

    today = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(today, dtime.min, tzinfo=timezone.utc)
    timestamp_ms = int(start_of_day.timestamp() * 1000)

    query = [TopicPartition(p.topic, p.partition, timestamp_ms) for p in parts]
    resolved = consumer.offsets_for_times(query, timeout=timeout)

    for tp in resolved:
        if tp.offset < 0:
            log.info("No messages since %s on %s[%d] — seeking to end",
                     start_of_day.isoformat(), tp.topic, tp.partition)
            continue
        consumer.seek(tp)
        log.info("Seeked %s[%d] to offset %d (%s UTC)",
                 tp.topic, tp.partition, tp.offset, start_of_day.isoformat())


def resolve_actual_departure_iso(event: dict) -> str | None:
    """
    Pick the best timestamp representing when the flight actually departed.
    Cascade: actual_departure → estimated_departure (if delay_minutes > 0
    or no other choice) → observed_at_utc.

    Note: when delay_minutes is precomputed by poller, the poller already
    used (actual or estimated) to compute it, so the same fields are
    present. We just pick the best available raw timestamp here.
    """
    for field in ("actual_departure", "estimated_departure", "observed_at_utc"):
        val = event.get(field)
        if val:
            return val
    # Last resort: use scheduled (it means we have no info, treat as on-time)
    return event.get("scheduled_departure")


def _compute_delay_minutes(event: dict) -> int | None:
    """
    Same cascade as stats-aggregator:
      1. delay_minutes precomputed by poller
      2. actual_departure - scheduled
      3. estimated_departure - scheduled
      4. observed_at_utc - scheduled
    """
    delay = event.get("delay_minutes")
    if isinstance(delay, (int, float)):
        return int(delay)

    sched = event.get("scheduled_departure")
    if not sched:
        return None
    try:
        sched_dt = datetime.fromisoformat(sched.replace("Z", "+00:00"))
    except ValueError:
        return None

    for field in ("actual_departure", "estimated_departure", "observed_at_utc"):
        val = event.get(field)
        if not val:
            continue
        try:
            ref_dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return int((ref_dt - sched_dt).total_seconds() / 60)
        except ValueError:
            continue
    return None


def build_notification(event: dict) -> dict | None:
    """Produce a notification dict ready to be published on flight.alerts."""
    flight_code = event.get("flight_code")
    if not flight_code:
        return None

    sched = event.get("scheduled_departure")
    actual = resolve_actual_departure_iso(event)
    if not sched or not actual:
        return None

    dest_iata = (event.get("destination_iata") or "").upper()
    info = IATA_AIRPORTS.get(dest_iata) if dest_iata else None
    if info and info.get("city"):
        destination = info["city"]
    else:
        destination = (
            event.get("destination_city")
            or event.get("destination_name")
            or dest_iata
            or "—"
        )

    text = (
        f"Flight {flight_code} to {destination}: "
        f"departed at {actual} (scheduled: {sched})"
    )

    delay = _compute_delay_minutes(event)

    return {
        "airport": event.get("airport"),
        "flight_code": flight_code,
        "destination_iata": event.get("destination_iata"),
        "destination_name": destination,
        "scheduled_departure": sched,
        "actual_departure": actual,
        "delay_minutes": delay,
        "text": text,
        "alert_day_utc": datetime.now(timezone.utc).date().isoformat(),
        "ts": time.time(),
    }


def start_notifier():
    log.info("Starting notifier")
    log.info("In: %s   Out: %s   GroupID: %s", TOPIC_IN, TOPIC_OUT, GROUP_ID)

    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "flight-notifier-consumer",
    })
    consumer.subscribe([TOPIC_IN])
    seek_to_start_of_today_utc(consumer, TOPIC_IN)

    producer = Producer({
        **kafka_ssl_base(),
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 1,
        "retries": 2147483647,
        "compression.type": "gzip",
        "client.id": "flight-notifier-producer",
    })

    # In-memory dedup: same flight emitted only once per scheduled departure
    sent_keys: set[tuple] = set()
    running = {"flag": True}

    def stop(*_):
        log.info("Shutdown signal received")
        running["flag"] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while running["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error("Malformed message at offset %d: %s — skipping", msg.offset(), e)
                consumer.commit(message=msg, asynchronous=False)
                continue

            if event.get("status") != "DEPARTED":
                # Non-DEPARTED events are ignored, but offset advances
                consumer.commit(message=msg, asynchronous=False)
                continue

            dedup_key = (event.get("airport"), event.get("flight_code"), event.get("scheduled_departure"))
            if dedup_key in sent_keys:
                consumer.commit(message=msg, asynchronous=False)
                continue

            notif = build_notification(event)
            if notif is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                producer.produce(
                    TOPIC_OUT,
                    key=(notif.get("airport") or "UNK").encode("utf-8"),
                    value=json.dumps(notif).encode("utf-8"),
                )
                producer.poll(0)
                sent_keys.add(dedup_key)
                log.info("Produced alert: %s", notif["text"])
                consumer.commit(message=msg, asynchronous=False)
            except BufferError:
                log.warning("Producer queue full, will retry on next poll")
                # Do NOT commit, message will be reprocessed
            except KafkaException as e:
                log.error("Failed to produce alert: %s", e)
                # Do NOT commit, message will be reprocessed

    except KafkaException as e:
        log.error("Fatal Kafka error: %s", e)
    finally:
        log.info("Closing consumer and flushing producer")
        producer.flush(timeout=10)
        consumer.close()


if __name__ == "__main__":
    start_notifier()