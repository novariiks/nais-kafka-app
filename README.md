# NAIS Kafka

A small demo of Kafka and OpenTelemetry on the Novari NAIS platform. A **Naisjob** publishes sensor readings to Kafka every two minutes, a **Kotlin / Spring Boot** consumer reads them, and a **React** frontend shows them live. One trace follows each event from the job, through Kafka, into the consumer.

```mermaid
flowchart LR
    Job[Naisjob\nnais-kafka-producer] -- "publish\n(traceparent header)" --> Topic[(Kafka\nflais.nais-kafka-events\n3 partitions)]
    Topic -- "@KafkaListener" --> Consumer[Kotlin consumer\nnais-kafka-consumer]
    Consumer -- Server-sent events --> Frontend[Nginx + React\nnais-kafka-frontend]
    Frontend --> Browser
    Job -. spans .-> Tempo[(Grafana Tempo)]
    Consumer -. spans .-> Tempo
```

## Run locally

Requirements: Docker with Compose. The container builds supply Node and the JDK.

```sh
docker compose up --build
```

- **http://localhost:3000**: the frontend.
- **http://localhost:3001**: Grafana with Tempo (Explore → Tempo). Every trace link in the UI opens here.

Compose starts Kafka, creates the topic, starts the consumer and frontend, and runs the producer once. Publish another batch with:

```sh
docker compose run --rm producer
```

Locally the containers run the OpenTelemetry Java agent themselves (the `local` Dockerfile target). On NAIS the platform injects it.

## Develop

Requirements: [mise](https://mise.jdx.dev) (installs Java and Node from `mise.toml`) and Docker.

```sh
mise install

# Kafka and Grafana only.
docker compose up -d kafka kafka-init lgtm

# Terminal 1: consumer on localhost:8080
./gradlew :consumer:bootRun

# Terminal 2: frontend on localhost:5173, proxying /api to the consumer
cd frontend && npm ci && npm run dev

# Publish a batch
./gradlew :producer:bootRun
```

Without the agent, local runs record no trace ids, so the UI shows no trace links.

## How it works

1. **Producer (Naisjob).** Every two minutes the job starts, publishes 10 events and exits. Each event is a reading from one of six simulated devices. The record key is the device id, so each device stays on one partition and in order. The run fails unless the broker acknowledges every record.
2. **Tracing.** Each run opens a `producer run` span. Because the job starts a new Kafka client every run, it first connects and fetches topic metadata in a `kafka connect` span. Against Aiven this takes seconds, and doing it before any event exists keeps that cost out of each event's latency. The agent's Kafka instrumentation then makes each send a child span and writes the W3C `traceparent` header into the record. The agent in the consumer continues that trace when `@KafkaListener` processes the record, so job → Kafka → consumer is one trace:

   ```
   producer run                                  nais-kafka-producer
   ├── kafka connect
   └── flais.nais-kafka-events publish           one per event
       └── flais.nais-kafka-events process       nais-kafka-consumer
   ```
3. **Consumer.** The listener keeps the latest 100 events in memory, with partition, offset, consuming pod and trace id, and pushes new events to browsers over server-sent events. State is per replica, so the app runs one replica. On partition assignment it replays the newest records of each partition, so a restart shows events immediately instead of after the next job run.
4. **Frontend.** Nginx serves the React app and proxies `/api` to the consumer, with buffering off for the event stream. Browsers deduplicate by event id and reconcile with `/api/events` every ten seconds.

| Endpoint (consumer) | Purpose |
| --- | --- |
| `GET /api/events` | Latest 100 consumed events |
| `GET /api/stream` | Server-sent events: `connected`, `event`, `heartbeat` |
| `GET /api/info` | Replica, topic, consumer group, events per partition, Grafana settings |

## Verify

```sh
./gradlew build                 # Embedded Kafka integration tests; no Docker needed
cd frontend && npm test && npm run build
```

To check trace propagation with the real agent, download the [OpenTelemetry Java agent](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases) and run:

```sh
./gradlew :consumer:test --rerun -PotelAgent=build/otel-agent.jar
./gradlew :producer:test --rerun -PotelAgent=build/otel-agent.jar
```

The spans are printed to the terminal. The consumer test also asserts that the consumer's trace id is the producer's. The producer test shows the run's span structure, from `kafka connect` to `producer run`.

## Deploy to Novari NAIS

Team `flais`, environment `test`, Kafka pool `novari-test`.

| Manifest | Resource |
| --- | --- |
| `.nais/topic.yaml` | Topic `nais-kafka-events` (`flais.nais-kafka-events` for clients) with write access for the producer and read access for the consumer |
| `producer/.nais/naisjob.yaml` | Naisjob, schedule `*/2 * * * *`, `concurrencyPolicy: Forbid` |
| `consumer/.nais/app.yaml` | Application without ingress; only the frontend may call it |
| `frontend/.nais/app.yaml` | Application with ingress https://nais-kafka.test.novari.cloud.nais.io |

Kafka credentials come from the variables NAIS injects (`KAFKA_BROKERS`, `KAFKA_KEYSTORE_PATH`, `KAFKA_TRUSTSTORE_PATH`, `KAFKA_CREDSTORE_PASSWORD`), read by the `nais` Spring profile. Both Kafka workloads enable `observability.autoInstrumentation` for Java. Traces are in [Grafana](https://grafana.novari.cloud.nais.io/explore), for example with `{resource.service.name="nais-kafka-consumer"}`.

The UI's trace links need the UID of the Tempo datasource for the environment. Each environment has its own; find them under `tracesDataSource.byEnvironment` in https://grafana.novari.cloud.nais.io/api/plugins/nais-apm-app/settings. The `nais` profile defaults to the one for `test`. For another environment, set `GRAFANA_TEMPO_DATASOURCE` in the consumer's manifest.

### GitHub Actions

Each deployable has its own workflow that builds the image with `nais/docker-build-push`, then deploys with `nais apply`. Workflows run on pushes to `main` that touch their paths, and can be started manually.

- `deploy-producer.yaml` and `deploy-consumer.yaml` apply the topic, then their workload. Either can be deployed first.
- `deploy-frontend.yaml` deploys the frontend.
- `ci.yaml` builds and tests on every push.

Before the first run, authorize this repository for team `flais` in Nais Console.

### Manual deployment

```sh
nais apply .nais/topic.yaml --environment test --team flais --wait
nais apply producer/.nais/naisjob.yaml --environment test --team flais --set spec.image=YOUR_PRODUCER_IMAGE --wait
nais apply consumer/.nais/app.yaml --environment test --team flais --set spec.image=YOUR_CONSUMER_IMAGE --wait
nais apply frontend/.nais/app.yaml --environment test --team flais --set spec.image=YOUR_FRONTEND_IMAGE --wait
```

## Demo ideas

- Watch a batch arrive every two minutes. Compare partitions and offsets for the same device.
- Open a trace link: one trace from the job's `producer run` to the consumer's `process` spans. Compare `kafka connect`, the cost of starting a Kafka client in a cron job, with the time the events spend in Kafka, shown as milliseconds on each event in the UI.
- Restart the consumer pod. The feed refills from Kafka right away.
- Look at consumer lag and Kafka client metrics on `/actuator/prometheus`.

## Repository

| Path | Purpose |
| --- | --- |
| `events/` | Shared event type and topic name |
| `producer/` | Naisjob: publishes one traced batch and exits |
| `consumer/` | Kotlin Spring Boot consumer, API and server-sent events |
| `frontend/` | React, TypeScript, Tailwind CSS and Nginx reverse proxy |
| `.nais/topic.yaml` | Kafka topic and access |
| `compose.yaml` | Local Kafka, Grafana LGTM and all three deployables |
| `mise.toml` | Java and Node versions |
| `.cplt.toml` | Sandbox permissions for AI agents running under cplt |
