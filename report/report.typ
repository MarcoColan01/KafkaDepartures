#import "@preview/basic-report:0.5.0": *

#show: it => basic-report(
  doc-category: "Final project for the Cloud Computing Technologies course (2025/26 academic year)",
  doc-title: "KakfaDepartures: a Real-Time Cloud-Native departure dashboard for three European airports (Amsterdam-Schiphol, Helsinki-Vantaa & Oslo-Gardermoen",
  author: "Marco Colangelo (67045A)",
  affiliation: "Università degli Studi di Milano, Dipartimento di Informatica \"Giovanni degli Antoni\" ",
  language: "en",
  compact-mode: true,
  heading-color: black,
  it
)
= Introduction 
This project implements a Cloud-Native architecture for real-time flight departures using Apache Kafka in KRaft mode. The system ingests live data from three European airports - Amsterdam-Schiphol, Helsinki-Vantaa and Oslo-Gardermoen - through their REST APIs (Schiphol Public Flight API for AMS, Finavia Public Flights API for HEL, Avinor XML feed for OSL). The heterogeneous data is normalized into a canonical schema and consumed in parallel by multiple microservices: 
- real-time visualization through a web dashboard with per-airport statistics;
- structured notifications of departed flights;
- a daily rolling counter of departures and average delays.
In this project, Kafka is the single source of truth for all derived state. The main focus is to guarantee three non-functional properties: fault tolerance, load balancing and transport-layer security.

= System Architecture
The infrastructure follows a decoupled microservice patter with all communication mediated by Apache Kafka. The cluster runs in KRaft mode with three brokers acting both as data nodes and as members of the metadata quorum.


= System Description
== Main Components
Below is a description of the main components of the system.
- Schiphol, Finavia and Avinor Pollers: three host-side Python adapters, one per aiport. Each polls its provider every 60 seconds, maintains a sliding window of 20 imminent flights, translates the airport-specific API format (paginated JSON for Schiphol, namespaced XML for Helsinki and attribute-based XML for Oslo) into a canonical FlightEvent schema, and forwards events via HTTP POSTs to the API-producer.
- API-producer: a FastAPI service exposing a POST /flight endpoint with Pydantic validation. Publishes events to the flight.telemetry topic with acks=all, enable.idempotence=True, infinite retries, and gzip compression. A custom ResilientProducer wrapper detects the idempotent producer's fatal state - that may occuper after prolonged broker unailavibility - and transparently reinstantiates it without container restart.
- Stats-aggregator: a Kafka Consumer and Producer that builds rolling counters of departed flights and average dalays per airport. On every boot, it seeks back to 00:00 UTC and rebuilds the day's state from the log. Snapshots are published on flight.stats every 10 seconds. The delay is computed through a cascading fallback: precomputed delay_minutes → actual_departure → estimated_departure → observed_at_utc (the time the poller observed the departure).
- Notifier: Kafka Consumer/Producer that emits a human-readable alert on flight.alerts whenever a flight transitions to DEPARTED, using the same delay cascade. Destination IATA codes are resolved to city names via the airportsdata library. An in-memory deduplication set guarantees one alert per flight.
= Non-Functional Properties 



= Application Showcases
