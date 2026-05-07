
import os
import json
import threading
import queue
import uuid
from datetime import datetime, timezone

from flask import Flask, Response, render_template
from confluent_kafka import Consumer
import airportsdata

from airline_codes import airline_name


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC_TELEMETRY = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
TOPIC_STATS = os.getenv("TOPIC_STATS", "flight.stats")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "flight.alerts")

GROUP_TELEMETRY = os.getenv("DASHBOARD_TELEMETRY_GROUP", f"dashboard-telemetry-{uuid.uuid4()}")
GROUP_STATS = os.getenv("DASHBOARD_STATS_GROUP", f"dashboard-stats-{uuid.uuid4()}")
GROUP_ALERTS = os.getenv("DASHBOARD_ALERTS_GROUP", f"dashboard-alerts-{uuid.uuid4()}")

AIRPORTS = ["AMS", "HEL", "OSL"]
BOARD_SIZE = int(os.getenv("BOARD_SIZE", "20"))
ALERT_FEED_MAX = int(os.getenv("ALERT_FEED_MAX", "200"))   # in-memory ring cap

webapp = Flask(__name__, static_folder="static", template_folder="template")


IATA_AIRPORTS = airportsdata.load("IATA")
print(f"[dashboard] airportsdata loaded: {len(IATA_AIRPORTS)} entries", flush=True)


state_lock = threading.Lock()

state = {
    code: {
        "flights": {},
        "stats": {
            "departed_today": 0,
            "avg_delay_minutes": None,
            "stats_day_utc": None,
        },
    }
    for code in AIRPORTS
}


alerts: list[dict] = []

sse_clients: list[queue.Queue] = []
sse_clients_lock = threading.Lock()

started = False


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



def enrich_event(event: dict) -> dict:
    enriched = dict(event)

    iata = event.get("airline_iata")
    full_airline = airline_name(iata)
    if full_airline:
        enriched["airline_name_full"] = full_airline
    else:
        enriched["airline_name_full"] = event.get("airline_name") or iata or "—"

    dest_iata = (event.get("destination_iata") or "").upper()
    info = IATA_AIRPORTS.get(dest_iata) if dest_iata else None
    if info:
        enriched["destination_name_full"] = info.get("name") or ""
        enriched["destination_city"] = info.get("city") or dest_iata
    else:
        enriched["destination_name_full"] = event.get("destination_name") or ""
        enriched["destination_city"] = dest_iata or "—"

    return enriched


def upsert_flight(event: dict) -> None:
    airport = event.get("airport")
    if airport not in state:
        return
    fc = event.get("flight_code")
    sd = event.get("scheduled_departure")
    if not fc or not sd:
        return
    key = f"{fc}|{sd}"

    enriched = enrich_event(event)

    with state_lock:
        if event.get("event_type") == "DELETE" or event.get("event_type") == "CANCELLED":
            state[airport]["flights"].pop(key, None)
        else:
            state[airport]["flights"][key] = enriched


def update_stats(event: dict) -> None:
    airport = event.get("airport")
    if airport not in state:
        return
    with state_lock:
        state[airport]["stats"] = {
            "departed_today": event.get("departed_today", 0),
            "avg_delay_minutes": event.get("avg_delay_minutes"),
            "stats_day_utc": event.get("stats_day_utc"),
        }


def add_alert(event: dict) -> None:
    """Add a new notification to the in-memory feed, today only."""
    today_utc = datetime.now(timezone.utc).date().isoformat()
    if event.get("alert_day_utc") and event["alert_day_utc"] != today_utc:
        return
    # Idempotency: skip if same (airport, flight_code, scheduled) is already present
    key = (event.get("airport"), event.get("flight_code"), event.get("scheduled_departure"))
    with state_lock:
        for existing in alerts:
            ek = (existing.get("airport"), existing.get("flight_code"), existing.get("scheduled_departure"))
            if ek == key:
                return
        alerts.insert(0, event)
        # Cap memory usage
        del alerts[ALERT_FEED_MAX:]


def board_snapshot() -> dict:
    now_utc = datetime.now(timezone.utc)
    out = {"airports": {}, "alerts": []}
    today_utc = now_utc.date().isoformat()

    with state_lock:
        for airport, bucket in state.items():
            flights = []
            for fl in bucket["flights"].values():
                sd = fl.get("scheduled_departure")
                if not sd:
                    continue
                try:
                    sd_dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if (sd_dt - now_utc).total_seconds() > -1800:
                    flights.append(fl)
            flights.sort(key=lambda f: f["scheduled_departure"])
            out["airports"][airport] = {
                "flights": flights[:BOARD_SIZE],
                "stats": bucket["stats"],
            }

        # Alerts: filter today only, sort by actual_departure DESC (most recent first)
        today_alerts = [a for a in alerts if a.get("alert_day_utc") == today_utc]
        today_alerts.sort(key=lambda a: a.get("actual_departure") or "", reverse=True)
        out["alerts"] = today_alerts

    return out


def push_to_clients(payload: str) -> None:
    with sse_clients_lock:
        clients = list(sse_clients)
    for q in clients:
        try:
            q.put_nowait(payload)
        except queue.Full:
            pass


def broadcast_state() -> None:
    payload = json.dumps(board_snapshot())
    push_to_clients(payload)


def telemetry_loop() -> None:
    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_TELEMETRY,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": "flight-dashboard-telemetry",
    })
    consumer.subscribe([TOPIC_TELEMETRY])
    print(f"[dashboard] telemetry consumer started, group={GROUP_TELEMETRY}", flush=True)
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"[dashboard] telemetry kafka error: {msg.error()}", flush=True); continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[dashboard] malformed telemetry: {e}", flush=True); continue
        upsert_flight(event)
        broadcast_state()


def stats_loop() -> None:
    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_STATS,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "client.id": "flight-dashboard-stats",
    })
    consumer.subscribe([TOPIC_STATS])
    print(f"[dashboard] stats consumer started, group={GROUP_STATS}", flush=True)
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"[dashboard] stats kafka error: {msg.error()}", flush=True); continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[dashboard] malformed stats: {e}", flush=True); continue
        update_stats(event)
        broadcast_state()


def alerts_loop() -> None:
    """
    Reads flight.alerts from the BEGINNING (auto.offset.reset=earliest) so
    that on dashboard boot we can rebuild today's notification feed. The
    add_alert() function discards messages from previous days.
    """
    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_ALERTS,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "client.id": "flight-dashboard-alerts",
    })
    consumer.subscribe([TOPIC_ALERTS])
    print(f"[dashboard] alerts consumer started, group={GROUP_ALERTS}", flush=True)
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"[dashboard] alerts kafka error: {msg.error()}", flush=True); continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[dashboard] malformed alert: {e}", flush=True); continue
        add_alert(event)
        broadcast_state()


def start_kafka_threads() -> None:
    global started
    if started: return
    threading.Thread(target=telemetry_loop, daemon=True).start()
    threading.Thread(target=stats_loop, daemon=True).start()
    threading.Thread(target=alerts_loop, daemon=True).start()
    started = True


@webapp.before_request
def before():
    start_kafka_threads()



@webapp.get("/healthcheck")
def health():
    return {"ok": True}


@webapp.get("/")
def index():
    return render_template("index.html", airports=AIRPORTS)


@webapp.get("/snapshot")
def snapshot():
    return board_snapshot()


@webapp.get("/stream")
def stream():
    client_q: queue.Queue = queue.Queue(maxsize=64)
    with sse_clients_lock:
        sse_clients.append(client_q)

    def event_stream():
        try:
            yield f"data: {json.dumps(board_snapshot())}\n\n"
            while True:
                payload = client_q.get()
                yield f"data: {payload}\n\n"
        finally:
            with sse_clients_lock:
                if client_q in sse_clients:
                    sse_clients.remove(client_q)

    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    webapp.run(host="0.0.0.0", port=8500, debug=False)