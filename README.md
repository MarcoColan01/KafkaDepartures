# KafkaDepartures: Live departures board for 3 European airports (Amsterdam-Schiphol, Helsinki-Vantaa & Oslo-Gardermoen) with Apache Kafka

This repository contains the final project for the Cloud Computing Technologies course, offered by the Master's Degree in Computer Science at the University of Milan in the 2025/26 academic year. The project's author is Marco Colangelo (ID 67045A).

**A cloud-native, real-time flight departures board system built around Apache Kafka in KRaft mode.**

![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20Docker-blue)
![Build](https://img.shields.io/badge/orchestration-Docker%20Compose-brightgreen)
![Security](https://img.shields.io/badge/security-mTLS-orange)

## Overview

KafkaDepartures ingests live departure data from three European airports - **Amsterdam-Schiphol (AMS)**, **Helsinki-Vantaa (HEL)**, **Oslo-Gardermoen (OSL)** - through their official public APIs, normalizes the heterogeneous formats into a canonical schema, and presents the resulting feed in real time through a web dashboard. The system implements stream-based microservices on top of a three-broker Kafka cluster (KRaft mode, no ZooKeeper), with fault tolerance, partition-based load balancing, and end-to-end mTLS as native architectural properties.

Report (in PDF format) describing the full design: `report.pdf`.

### Key Features
- **Real-time multi-hub board** for AMS, HEL, OSL with the next 20 scheduled departures.
- **Per-airport statistics:** departed flights and average delay, refreshed every 10 seconds. **IMPORTANT CLARIFICATION:** Statistics are for the current day and are accumulated while the application is running, and are restored to their last updated state at each startup (they do not take into account flights that departed while the application was not running). At midnight (00:00 UTC), all statistics are reset.
- **Scrollable departure feed** with structured notifications of today's departed flights.
- **Heterogeneous API adapters:** JSON paginated (AMS), namespaced XML (HEL), attribute-based XML (OSL).
- **mTLS-encrypted Kafka** with a self-signed internal CA.
- **Idempotent, resilient producer** with auto-recovery from fatal states.
- **Stateless aggregator** that rebuilds teh day's stats by replaying the log from 00:00 UTC.

## Architecture
![Architecture](report/architecture.png)

### Technology stack
**Data sources (external):**
- <a href="https://www.schiphol.nl/en/developer-center/" target="_blank">Schiphol Public Flights API (paginated JSON)</a>
- <a href="https://apiportal.finavia.fi/" target="_blank">Finavia Public Flights API (namespaced XML)</a>
- <a href="https://api2-developer.avinor.no/" target="_blank">Avinor XML Feed (attribute-based XML)</a>

**Internal services:**
- <a href="https://kafka.apache.org/" target="_blank"> Apache Kafka 3.7 </a> (KRaft mode, 3 brokers, replication factor of 3) 
- <a href = "https://fastapi.tiangolo.com/" target="_blank"> FastAPI </a> + <a href = "https://uvicorn.dev/" target="_blank"> uvicorn </a> (HTTP gateway)
- <a href = "https://docs.confluent.io/kafka-clients/python/current/overview.html" target="_blank"> confluent-kafka-python </a> (clients)
- <a href = "https://flask.palletsprojects.com/en/stable/" target="_blank"> Flask </a> + <a href = "https://fastapi.tiangolo.com/tutorial/server-sent-events/" target="_blank"> Server-Sent Events </a> (dashboard)
- `airportsdata` library (for IATA code → city resolution)

**Infrastructure:**
- Docker + Docker Compose for orchestration
- OpenSSL for the internal PKI
- Self-signed CA `FlightFlowCA` and mTLS on all listeners

## Project structure
```text
cloud-computing-project/
├── 📁 scripts/                       # Host-side adapters (one venv each)
│   ├── 📁 common/                    # Shared FlightEvent dataclass
│   ├── 📁 schiphol_poller/           # Amsterdam poller
│   ├── 📁 helsinki_poller/           # Helsinki poller
│   ├── 📁 oslo_poller/               # Oslo poller
│   └── rogue_poller.py               # Security test (gitignored)
├── 📁 security/
│   └── generate_certs.sh             # PKI bootstrap script
├── 📁 services/                      # Containerised microservices
│   ├── 📁 api-producer/              # HTTP → Kafka gateway
│   ├── 📁 stats-aggregator/          # Daily counters
│   ├── 📁 notifier/                  # DEPARTED → flight.alerts
│   └── 📁 dashboard/                 # Flask + SSE web UI
├── 📄 docker-compose.yml             # Cluster orchestration
├── 📄 .env.example                   # Environment template
├── 📄 report.pdf                     # Full project report
└── 📄 README.md                      # This file
```

## Getting started
### Prerequisites
- **Docker** 24.0 or later, **Docker Compose** v2
    - <a href = "https://docs.docker.com/desktop/setup/install/windows-install/" >Windows</a>
    - <a href = "https://docs.docker.com/desktop/setup/install/mac-install/" >macOS</a>
    - <a href = "https://docs.docker.com/desktop/setup/install/linux/">Linux</a>
- **Python 3.11** or later
- **OpenSSL** (any recent version)
- API credentials:
    - Schiphol Public Flight API: `app_id` + `app_key` (free signup at <a href = "https://developer.schiphol.nl/signup" >developer.schiphol.nl</a>)
    - Finavia Public Flights API: `app_key` (free signup at <a href = "https://apiportal.finavia.fi/signup">apigw.finavia.fi</a>)
    - Avinor XML Feed: no auth required

### Installation
**1. Clone this repository**

Linux / macOS / Windows:
```bash
git clone https://github.com/MarcoColan01/cloud-computing-project.git
cd cloud-computing-project
```

**2. Generate the PKI (self-signed CA + broker / client credentials)**

Linux / macOS:
```bash
cd security
chmod +x generate_certs.sh
./generate_certs.sh
cd ..
```

Windows (PowerShell, requires WSL (reccomended) or Git Bash):
```powershell
cd security
bash generate_certs.sh
cd ..
```

The script generates `FlightFlowCA`, three broker keystores (PKCS12), and the client PEM key-cert pair under `security/client-creds/`.

**3. Configure environment variables**

Copy this template:

Linux / macOS:
```bash
cp .env.example .env
```

Windows (PowerShell):
```powershell
Copy-Item .env.example .env
```

Edit `.env` and fill in:

```bash
KAFKA_SSL_PASSWORD=<the password used by generate_certs.sh>
SCHIPHOL_APP_ID=<your Schiphol app_id>
SCHIPHOL_APP_KEY=<your Schiphol app_key>
FINAVIA_APP_KEY=<your Finavia app_key>
```

### Run the cluster

**1. Build and start all services**

Linux / macOS / Windows (PowerShell):
```bash
docker compose up -d --build
docker compose ps
```
Wait until every container shows `(healthy)`. First boot takes 30–60 seconds for brokers to form the KRaft quorum.

**2. Verify the cluster is up**

Linux / macOS:
```bash
curl http://localhost:8000/healthcheck
curl http://localhost:8500/healthcheck
```

Windows (PowerShell):
```powershell
curl.exe http://localhost:8000/healthcheck
curl.exe http://localhost:8500/healthcheck
```

both should respond with `{"ok": true, ...}`.

### Start the pollers

Each poller runs on the host with its own virtual envirnoment.

**Amsterdam Schiphol** 

Linux / macOS:
```bash
cd scripts/schiphol_poller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python poller.py
```

Windows (PowerShell):
```powershell
cd scripts\schiphol_poller
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python poller.py
```

**Helsinki Vantaa** 

Linux / macOS:
```bash
cd scripts/helsinki_poller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python poller.py
```

Windows (PowerShell):
```powershell
cd scripts\helsinki_poller
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python poller.py
```

**Oslo Gardermoen** 

Linux / macOS:
```bash
cd scripts/oslo_poller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python poller.py
```

Windows (PowerShell):
```powershell
cd scripts\oslo_poller
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python poller.py
```

Each poller logs one line per cycle, e.g.:
2026-05-07 12:00:01 [INFO] Cycle 1.4s | trk:20 sent:20 fail:0 deleted:1 updated:19 added:1

### Open the dashboard

Browse to: 

- **Dashboard**: http://localhost:8500
- **Kafka-UI** (admin): http://localhost:8080

Three independent boards (AMS, HEL, OSL) populate within one polling cycle (~60 s).

## Testing the non-functional properties

The system was validated against three categories of guarantees. Reproduction commands below.

### Fault tolerance

**Stop one broker** (system stays fully operational, ISR drops to 2):

Linux / macOS / Windows:
```bash
docker compose stop kafka-2     #or kafka-1 kafka-3
# wait ~10 seconds, observe Kafka-UI: partition leaders re-elected, dashboard still live
docker compose start kafka-2    #or kafka-1 kafka-3
```

**Stop two brokers** (writes held in producer queue, recovered on restart):

```bash
docker compose stop kafka-1 kafka-3
# dashboard stops updating; producer queues messages internally
docker compose start kafka-1 kafka-3
# queued messages are delivered automatically when ISR is restored
```

**Crash a stateful consumer** (rebuilt from log at boot):

```bash
docker compose kill stats-aggregator
docker compose up -d stats-aggregator
docker compose logs stats-aggregator --tail 30
```

Expected: `Seeked flight.telemetry[N] to offset M (00:00 UTC)` followed by counters identical to pre-crash state plus any departures during downtime.

###  Load balancing

**Scale a partition-based consumer to 3 replicas**:

```bash
docker compose up -d --scale stats-aggregator=3
# Kafka-UI → Consumers → stats-aggregator-group: 6 partitions split 2-2-2
docker compose up -d --scale stats-aggregator=1
```

**Broadcast pattern for the dashboard**: open `http://localhost:8500` in two browser tabs — both receive every update independently (each tab is its own consumer group).

###  Security (mTLS)

**Try connecting without valid credentials**:

Linux / macOS:
```bash
cd scripts/schiphol_poller
source .venv/bin/activate
cd ../..
python scripts/rogue_poller.py
```

Windows (PowerShell):
```powershell
cd scripts\schiphol_poller
.venv\Scripts\Activate.ps1
cd ..\..
python scripts\rogue_poller.py
```

All three attack scenarios (plain TCP, TLS without client cert, TLS with self-signed cert) are rejected at the transport layer.

**Verify wire-level encryption** (Linux only, requires docker network access):
```bash
docker run --rm --net=container:cloud-computing-project-kafka-1-1 nicolaka/netshoot \
    tcpdump -A -i any -n port 9093 -c 50
```

No application string (flight code, IATA, JSON) is observable in the packet payload.

##  Kafka topics

| Topic              | Partitions | Replication | Description                              |
| ------------------ | ---------- | ----------- | ---------------------------------------- |
| `flight.telemetry` | 6          | 3           | Live flight events (UPSERT / DELETE)     |
| `flight.stats`     | 3          | 3           | Per-airport rolling counters             |
| `flight.alerts`    | 3          | 3           | Departure notifications                  |

All topics have `min.insync.replicas=2`.

##  Shutdown

```bash
docker compose down
```

Add `-v` to also remove the persistent broker volumes.

##  Troubleshooting

**The brokers are stuck in `unhealthy`**: usually the PKI was not generated. Run `security/generate_certs.sh` and `docker compose up -d --build`.

**Poller logs `422 Unprocessable Entity`**: the airport code is not yet whitelisted in `services/api-producer/app.py`. Edit the `Airport = Literal[...]` line and rebuild the API-producer.

**Poller logs `Connection refused`**: the API-producer container is not running yet. Wait for `docker compose ps api-producer` to show `(healthy)`.

**Dashboard shows old flights after restart**: the host-side state file is stale. Already removed in current version; if upgrading from older revisions, delete `scripts/*/poller_state*` files.