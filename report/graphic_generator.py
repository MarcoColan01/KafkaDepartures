'''
from graphviz import Digraph

dot = Digraph(
    "FlightFlow",
    comment="Flight Flow architecture",
    format="png",
)

# Global style: clean, vertical, white background
dot.attr(rankdir="TB", bgcolor="white", fontname="Helvetica", fontsize="11")
dot.attr("node", fontname="Helvetica", fontsize="10",
         style="filled", fillcolor="white", color="black", penwidth="1.2")
dot.attr("edge", fontname="Helvetica", fontsize="9", color="#333333")

# ---------------------------------------------------------------------------
# External data sources (top)
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_sources") as c:
    c.attr(label="EXTERNAL DATA SOURCES",
           style="dashed", color="gray50", fontsize="10")
    c.node("schiphol_api", "Schiphol API", shape="box3d")
    c.node("finavia_api",  "Finavia API", shape="box3d")
    c.node("avinor_api",   "Avinor XML Feed", shape="box3d")

# ---------------------------------------------------------------------------
# Pollers (host-side adapters)
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_pollers") as c:
    c.attr(label="HOST-SIDE ADAPTERS (pollers)",
           style="dashed", color="gray50", fontsize="10")
    c.node("ams_poller", "AMS Poller",   shape="component", fillcolor="#fff7e6")
    c.node("hel_poller", "HEL Poller",   shape="component", fillcolor="#fff7e6")
    c.node("osl_poller", "OSL Poller",   shape="component", fillcolor="#fff7e6")

# ---------------------------------------------------------------------------
# Docker network cluster
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_docker") as c:
    c.attr(label="DOCKER NETWORK  (flight-net)",
           style="rounded,filled", fillcolor="#f5f5f5",
           color="gray30", fontsize="10")

    # API gateway
    c.node("api_producer", "API-Producer",
           shape="box", fillcolor="#e3f2fd")

    # Kafka cluster (subcluster within Docker)
    with c.subgraph(name="cluster_kafka") as k:
        k.attr(label="KAFKA CLUSTER (KRaft, RF=3, min ISR=2)",
               style="rounded,filled", fillcolor="#fff8e1",
               color="orange", fontsize="10")
        k.node("kafka1", "kafka-1",  shape="cylinder", fillcolor="#ffe0b2")
        k.node("kafka2", "kafka-2",  shape="cylinder", fillcolor="#ffe0b2")
        k.node("kafka3", "kafka-3",  shape="cylinder", fillcolor="#ffe0b2")

        # Topics as a single labeled box hanging below the brokers
        k.node("topics",
               "TOPICS\n• flight.departures\n"
               "• flight.statistics\n"
               "• flight.departed_flights",
               shape="folder", fillcolor="#fff3cd")

        k.edge("kafka1", "topics", style="invis")
        k.edge("kafka2", "topics", style="invis")
        k.edge("kafka3", "topics", style="invis")

    # Consumers
    c.node("stats",     "Stats-Aggregator",  shape="box", fillcolor="#e8f5e9")
    c.node("notifier",  "Notifier",          shape="box", fillcolor="#e8f5e9")
    c.node("dashboard", "Dashboard", shape="box", fillcolor="#e8f5e9")

    c.node("kafkaui",   "Kafka-UI\n(admin)", shape="box", fillcolor="#fce4ec")

# ---------------------------------------------------------------------------
# End user (bottom right)
# ---------------------------------------------------------------------------
dot.node("user", "User browser", shape="ellipse", fillcolor="white")

# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------
# APIs → pollers
dot.edge("schiphol_api", "ams_poller", label="HTTPS")
dot.edge("finavia_api",  "hel_poller", label="HTTPS")
dot.edge("avinor_api",   "osl_poller", label="HTTPS")

# Pollers → API-producer
dot.edge("ams_poller", "api_producer", label="HTTP POST")
dot.edge("hel_poller", "api_producer", label="HTTP POST")
dot.edge("osl_poller", "api_producer", label="HTTP POST")

# API-producer → topics
dot.edge("api_producer", "topics",
         label="produce\n(mTLS, acks=all)", color="#1565c0")

# Topics → consumers
dot.edge("topics", "stats",
         label="consume telemetry\n(static group)", color="#2e7d32")
dot.edge("topics", "notifier",
         label="consume telemetry\n(static group)", color="#2e7d32")
dot.edge("topics", "dashboard",
         label="consume all topics\n(random group = broadcast)",
         color="#2e7d32")

# Consumers that also produce (back to topics)
dot.edge("stats",    "topics",
         label="produce stats", color="#1565c0", style="dashed")
dot.edge("notifier", "topics",
         label="produce alerts", color="#1565c0", style="dashed")

# Kafka-UI also consumes via mTLS
dot.edge("topics", "kafkaui",
         label="admin\n(mTLS)", color="#c2185b", style="dotted")

# Dashboard → user
dot.edge("dashboard", "user", label="SSE stream\n(HTML / JSON)")

# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------
dot.render("flight_flow_architecture", cleanup=True)
print("Generated: flight_flow_architecture.png")

'''


from graphviz import Digraph

dot = Digraph(
    "FlightFlow",
    comment="Flight Flow architecture",
    format="png",
)

# Global style — clean and compact like Galliano's
dot.attr(rankdir="TB", bgcolor="white",
         fontname="Helvetica-Bold", fontsize="11", ranksep="0.55", nodesep="0.35")
dot.attr("node",
         fontname="Helvetica-Bold", fontsize="10",
         style="filled", color="black", penwidth="1.4",
         shape="box", margin="0.18,0.10")
dot.attr("edge",
         fontname="Helvetica", fontsize="9", color="black", penwidth="1.2")

# Palette (soft but distinguishable)
COL_SOURCE   = "#fff3cd"   # pale amber — external data
COL_ADAPTER  = "#d4edda"   # pale green — host-side pollers
COL_GATEWAY  = "#cfe2ff"   # pale blue  — API gateway
COL_BROKER   = "#f8d7da"   # pale red   — Kafka brokers
COL_TOPIC    = "#e2e3e5"   # neutral gray — topics box
COL_CONSUMER = "#d1ecf1"   # pale cyan  — derived consumers
COL_ADMIN    = "#e7d3f0"   # pale purple — admin tools
COL_USER     = "white"

# ---------------------------------------------------------------------------
# External data sources (top)
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_sources") as c:
    c.attr(label="EXTERNAL DATA SOURCES",
           style="dashed", color="gray40", fontsize="10")
    c.node("schiphol_api", "SCHIPHOL API",
           fillcolor=COL_SOURCE)
    c.node("finavia_api",  "FINAVIA API",
           fillcolor=COL_SOURCE)
    c.node("avinor_api",   "AVINOR XML FEED",
           fillcolor=COL_SOURCE)

# ---------------------------------------------------------------------------
# Pollers (host-side adapters)
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_pollers") as c:
    c.attr(label="HOST-SIDE ADAPTERS (POLLERS)",
           style="dashed", color="gray40", fontsize="10")
    c.node("ams_poller", "AMS POLLER", fillcolor=COL_ADAPTER)
    c.node("hel_poller", "HEL POLLER", fillcolor=COL_ADAPTER)
    c.node("osl_poller", "OSL POLLER", fillcolor=COL_ADAPTER)

# ---------------------------------------------------------------------------
# Docker network cluster
# ---------------------------------------------------------------------------
with dot.subgraph(name="cluster_docker") as c:
    c.attr(label="DOCKER NETWORK  (flight-net)",
           style="dashed,rounded", color="gray30", fontsize="10")

    # API gateway
    c.node("api_producer",
           "API-PRODUCER",
           fillcolor=COL_GATEWAY)

    # Kafka cluster (sub-subgraph)
    with c.subgraph(name="cluster_kafka") as k:
        k.attr(label="KAFKA CLUSTER",
               style="rounded", color="black", fontsize="10")
        k.node("kafka1", "KAFKA-1", fillcolor=COL_BROKER)
        k.node("kafka2", "KAFKA-2", fillcolor=COL_BROKER)
        k.node("kafka3", "KAFKA-3", fillcolor=COL_BROKER)

        k.node("topics",
               "TOPICS:\n• flight.departures\n"
               "• flight.statistics\n"
               "• flight.departed_flights",
               fillcolor=COL_TOPIC, shape="note")

        # Invisible edges to force vertical alignment of topics below brokers
        k.edge("kafka1", "topics", style="invis")
        k.edge("kafka2", "topics", style="invis")
        k.edge("kafka3", "topics", style="invis")

    # Consumers
    c.node("stats",     "STATS-AGGREGATOR",          fillcolor=COL_CONSUMER)
    c.node("notifier",  "NOTIFIER",                  fillcolor=COL_CONSUMER)
    c.node("dashboard", "DASHBOARD\nFlask + SSE",    fillcolor=COL_CONSUMER)

    # Admin
    c.node("kafkaui", "KAFKA-UI\nadmin", fillcolor=COL_ADMIN)

# ---------------------------------------------------------------------------
# End user
# ---------------------------------------------------------------------------
dot.node("user", "USER BROWSER", shape="ellipse", fillcolor=COL_USER)

# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------
# External APIs → pollers
dot.edge("schiphol_api", "ams_poller", label="HTTPS")
dot.edge("finavia_api",  "hel_poller", label="HTTPS")
dot.edge("avinor_api",   "osl_poller", label="HTTPS")

# Pollers → API-producer
dot.edge("ams_poller", "api_producer", label="HTTP POST")
dot.edge("hel_poller", "api_producer", label="HTTP POST")
dot.edge("osl_poller", "api_producer", label="HTTP POST")

# API-producer → topics
dot.edge("api_producer", "topics",
         label="produce  (mTLS, acks=all)",
         color="#1565c0", fontcolor="#1565c0", penwidth="1.5")

# Topics → consumers
dot.edge("topics", "stats",
         label="consume departures\nstatic group",
         color="#2e7d32", fontcolor="#2e7d32")
dot.edge("topics", "notifier",
         label="consume departures\nstatic group",
         color="#2e7d32", fontcolor="#2e7d32")
dot.edge("topics", "dashboard",
         label="consume ALL\nrandom group (broadcast)",
         color="#2e7d32", fontcolor="#2e7d32")

# Consumers that also produce
dot.edge("stats",    "topics",
         label="produce statistics",
         color="#1565c0", fontcolor="#1565c0", style="dashed")
dot.edge("notifier", "topics",
         label="produce departed_flights",
         color="#1565c0", fontcolor="#1565c0", style="dashed")

# Kafka-UI ↔ topics (admin)
dot.edge("topics", "kafkaui",
         label="admin (mTLS)",
         color="#7b1fa2", fontcolor="#7b1fa2", style="dotted")

# Dashboard → user
dot.edge("dashboard", "user",
         label="SSE stream\nHTML / JSON")

# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------
dot.render("flight_flow_architecture", cleanup=True)
print("Generated: flight_flow_architecture.png")