# Distributed Workflow Engine

A production-shaped Saga-pattern orchestrator built with Spring Boot, PostgreSQL, Apache Kafka, and Redis. It accepts declarative workflow definitions, executes their steps asynchronously against downstream services, retries transient failures with backoff, and rolls back completed work via compensation when a step exhausts its retries.

---

## Table of Contents

- [Architecture](#architecture)
- [How a Workflow Executes](#how-a-workflow-executes)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start (Docker Compose)](#quick-start-docker-compose)
- [Running Locally Against Docker Infra](#running-locally-against-docker-infra)
- [Configuration Reference](#configuration-reference)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Observability](#observability)
- [Scope and Limitations](#scope-and-limitations)

---

## Architecture

```text
                                        ┌─────────────────────────┐
                                        │         Client          │
                                        │  (Postman / curl / UI)  │
                                        └────────────┬────────────┘
                                                     │
                                            REST (HTTP/JSON)
                                                     │
                                                     ▼

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                      workflow-orchestrator (Spring Boot)                                     │
│                                                                                              │
│  ┌──────────────┐     ┌──────────────────────────┐     ┌─────────────────────┐              │
│  │ Controller   │────▶│ WorkflowOrchestration    │────▶│ StepDispatchService │              │
│  │ Layer        │     │ Service                  │     └──────────┬──────────┘              │
│  └──────────────┘     └────────────┬─────────────┘                │                         │
│                                    │                              │                         │
│                        ┌───────────▼───────────┐                  │                         │
│                        │ Distributed Lock      │                  │                         │
│                        │ Service (Redisson)    │                  │                         │
│                        └───────────┬───────────┘                  │                         │
│                                    │                              │                         │
│                        ┌───────────▼───────────┐                  │                         │
│                        │ CompensationService   │                  │                         │
│                        └───────────┬───────────┘                  │                         │
│                                    │                              │                         │
│                                    ▼                              ▼                         │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                  Repository Layer (Spring Data JPA)                                   │  │
│  └───────────────────────────────────┬────────────────────────────────────────────────────┘  │
│                                      │                                                       │
│  ┌────────────────────┐  ┌───────────▼───────────┐  ┌────────────────────┐                  │
│  │ TaskResultListener │  │ StepRetryScheduler   │  │ CompensationResult │                  │
│  │ (Kafka)            │  │ (@Scheduled poll)    │  │ Listener (Kafka)   │                  │
│  └─────────┬──────────┘  └───────────┬───────────┘  └─────────┬──────────┘                  │
└────────────┼─────────────────────────┼─────────────────────────┼─────────────────────────────┘
             │                         │                         │
             │ consumes                │ redispatches           │ consumes
             ▼                         ▼                         ▼

     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
     │ workflow.task   │     │ workflow.task   │     │ workflow.compensation│
     │ .result         │◀────│ .dispatch       │────▶│ .result             │
     └─────────────────┘     └────────┬────────┘     └─────────────────────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ Downstream Services     │
                         │                         │
                         │ • reserve-inventory     │
                         │ • charge-payment        │
                         └────────────┬────────────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ workflow.compensation   │
                         │ .dispatch               │
                         └────────────┬────────────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ Compensation Handlers   │
                         │                         │
                         │ • release-inventory     │
                         │ • refund-payment        │
                         └─────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Infrastructure                                            │
│                                                                                              │
│  ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐                  │
│  │ PostgreSQL         │   │ Apache Kafka       │   │ Redis              │                  │
│  │                    │   │                    │   │ (Redisson)         │                  │
│  │ workflow_definition│   │ 4 topics           │   │ Distributed locks  │                  │
│  │ workflow_step_def  │   │ 6 partitions each  │   │ per workflow       │                  │
│  │ workflow_instance  │   │                    │   │ instance           │                  │
│  │ step_execution     │   │                    │   │                    │                  │
│  │                    │   │                    │   │                    │                  │
│  │ Flyway migrations  │   │ Durable messaging  │   │ Lock coordination  │                  │
│  │ JSONB payloads     │   │ Ordered processing │   │ Watchdog renewal   │                  │
│  └────────────────────┘   └────────────────────┘   └────────────────────┘                  │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```
**Layering discipline enforced throughout:**

```
controller/          → REST endpoints only. Depends on service interfaces.
service/              → interfaces only (contracts, no logic)
service/impl/         → @Service implementations (all business logic lives here)
repository/           → Spring Data JPA interfaces only
repository/impl/      → custom repository implementations (native queries)
model/                → JPA entities + state enums (WorkflowStatus, StepStatus)
dto/request           → inbound REST payloads
dto/response          → outbound REST payloads
dto/messaging         → Kafka message contracts
config/               → Kafka, Redis, JPA, async, Jackson configuration
exception/            → domain exceptions + global @RestControllerAdvice
worker/                → Kafka @KafkaListener consumers + @Scheduled retry sweeper
```

---

## How a Workflow Executes

```
1. POST /api/v1/workflow-definitions
      │  Define an ordered list of steps, each with a task name,
      │  target Kafka topic, optional compensation task, retry limit.
      ▼
2. POST /api/v1/workflow-instances
      │  Starts a run. Instance persisted with status = RUNNING.
      │  First step dispatched immediately to its target topic.
      ▼
3. Downstream service consumes from its topic, does the work,
   publishes a TaskResultMessage to workflow.task.result.
      │
      ▼
4. TaskResultListener consumes the result:
      │
      ├── successful=true  → advance to next step, or mark COMPLETED
      │                       if this was the last step.
      │
      └── successful=false → increment attempt count.
             │
             ├── within retry limit → status=FAILED, nextRetryAt set.
             │                         StepRetryScheduler picks it up
             │                         and re-dispatches after backoff.
             │
             └── retry limit exceeded → instance status=COMPENSATING.
                                          CompensationService dispatches
                                          compensation for the most
                                          recently completed step.
      ▼
5. Compensation cascades backward through every COMPLETED step that
   declares a compensationTaskName, until none remain.
   Instance status becomes COMPENSATED.
```

Every state mutation above is wrapped in a Redis-backed distributed lock keyed by the workflow instance ID, so concurrent Kafka consumer threads — even across multiple application instances — cannot race on the same instance's state.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Application framework | Spring Boot 3.3 (Java 17) | Declarative transactions, mature Kafka/Redis integration, clean DI for strict layering |
| System of record | PostgreSQL 16 | MVCC-backed optimistic locking, native JSONB for workflow payloads |
| Schema management | Flyway | Versioned, auditable migrations; `ddl-auto: validate` catches entity/schema drift immediately |
| Async transport | Apache Kafka | Partition-ordered delivery per instance, durable replay, idempotent producers |
| Distributed locking | Redis + Redisson | Watchdog-renewed lock leases remove manual lease-tuning |
| Containerization | Docker Compose | One-command reproducible topology with health-check-gated startup |

---

## Project Structure

```
workflow-engine/
├── pom.xml
├── Dockerfile
├── docker-compose.yml
├── src/
│   ├── main/
│   │   ├── java/com/enginecorp/workfloworchestrator/
│   │   │   ├── WorkflowOrchestratorApplication.java
│   │   │   ├── controller/
│   │   │   ├── service/            (+ impl/)
│   │   │   ├── repository/         (+ impl/)
│   │   │   ├── model/
│   │   │   ├── dto/
│   │   │   │   ├── request/
│   │   │   │   ├── response/
│   │   │   │   └── messaging/
│   │   │   ├── config/
│   │   │   ├── exception/
│   │   │   └── worker/
│   │   └── resources/
│   │       ├── application.yml
│   │       └── db/migration/
│   │           ├── V1__create_workflow_definition_tables.sql
│   │           └── V2__create_workflow_instance_tables.sql
│   └── test/
│       └── java/com/enginecorp/workfloworchestrator/
│           ├── service/impl/WorkflowOrchestrationServiceImplTest.java
│           └── WorkflowInstanceIT.java
```

---

## Prerequisites

- **Docker Desktop** (with the Engine running — verify with `docker info`)
- **JDK 17** (only needed if running outside Docker)
- **Maven wrapper** (`mvnw` / `mvnw.cmd`) — included; if missing, generate with `mvn wrapper:wrapper`

---

## Quick Start (Docker Compose)

This brings up the entire system — Postgres, Zookeeper, Kafka, Redis, a Kafka UI, and the application itself — with one command.

```bash
# 1. Clone and enter the project
cd workflow-engine

# 2. Build the app image and start everything
docker compose up -d --build

# 3. Confirm all six services are healthy
docker compose ps

# 4. Watch the application boot (Flyway migrations, Kafka listeners, Tomcat)
docker compose logs -f workflow-orchestrator
```

Wait for this line in the logs before sending requests — it confirms the app is actually ready, not just started:

```
Tomcat started on port 8080 (http) with context path '/'
```

### Verify it's working

```bash
curl -X POST http://localhost:8080/api/v1/workflow-definitions \
  -H "Content-Type: application/json" \
  -d '{
        "name": "order-fulfillment",
        "description": "Reserve inventory then charge payment",
        "steps": [
          { "taskName": "reserve-inventory", "compensationTaskName": "release-inventory", "targetTopic": "inventory.tasks", "retryLimit": 2, "timeoutMs": 15000 },
          { "taskName": "charge-payment", "compensationTaskName": "refund-payment", "targetTopic": "payment.tasks", "retryLimit": 2, "timeoutMs": 15000 }
        ]
      }'
```

```bash
curl -X POST http://localhost:8080/api/v1/workflow-instances \
  -H "Content-Type: application/json" \
  -d '{ "workflowDefinitionId": "<id-from-previous-response>", "payload": { "orderId": "ORD-1001" } }'
```

```bash
curl http://localhost:8080/api/v1/workflow-instances/<instance-id>
```

### Services and ports

| Service | Port | Purpose |
|---|---|---|
| `workflow-orchestrator` | `8080` | The application's REST API |
| `postgres` | `5432` | Database |
| `kafka` | `9092` | Broker (external) |
| `kafka-ui` | `8090` | Browse topics, messages, consumer groups |
| `redis` | `6379` | Distributed lock backend |

### Simulating a downstream task result

No real downstream services are included — this engine dispatches to Kafka topics that any real service would consume. To simulate one manually for local testing:

```bash
docker exec -it workflow-engine-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic workflow.task.result
```

Paste a line and press Enter:

```json
{"workflowInstanceId":"<instance-id>","stepExecutionId":"<step-id-from-GET-response>","successful":true,"errorMessage":null,"resultPayload":{}}
```

Re-run the `GET /api/v1/workflow-instances/<instance-id>` call to see the instance advance to the next step.

### Tear down

```bash
docker compose down       # stop containers, keep data
docker compose down -v    # stop containers, wipe the Postgres volume too
```

---

## Running Locally Against Docker Infra

For faster iteration during development, run the app on your host machine while only the infrastructure runs in Docker.

```bash
# Start infra only
docker compose up -d postgres zookeeper kafka redis kafka-ui

# Run the app directly — application.yml defaults already match the compose file
./mvnw spring-boot:run
```

No environment variables are needed for this mode — `application.yml`'s `${DB_HOST:localhost}`-style defaults line up with the ports Docker Compose publishes to your host.

---

## Configuration Reference

All configuration lives in `src/main/resources/application.yml`, using `${VARIABLE:default}` placeholders. Override via environment variables, an IDE run configuration, or a `.env` file consumed by `docker-compose.yml`.

| Variable | Default | Used By |
|---|---|---|
| `DB_HOST` | `localhost` | Postgres connection |
| `DB_PORT` | `5432` | Postgres connection |
| `DB_NAME` | `workflow_engine` | Postgres connection |
| `DB_USERNAME` | `workflow_user` | Postgres connection |
| `DB_PASSWORD` | `workflow_pass` | Postgres connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka producer/consumer |
| `REDIS_HOST` | `localhost` | Redisson client |
| `REDIS_PORT` | `6379` | Redisson client |

> **Never commit real credentials.** For any non-local environment, source `DB_PASSWORD` from a secrets manager (AWS Secrets Manager, Kubernetes Secret, HashiCorp Vault) that populates the environment variable at container start — the app doesn't need to know or care where the value came from.

---

## API Reference

### Workflow Definitions

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/workflow-definitions` | Create a new (or new-version) workflow definition |
| `GET` | `/api/v1/workflow-definitions/{id}` | Fetch a single definition |
| `GET` | `/api/v1/workflow-definitions` | List all active definitions |

### Workflow Instances

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/workflow-instances` | Start a new run of a definition |
| `GET` | `/api/v1/workflow-instances/{id}` | Get full current state, including step history |

---

## Testing

```bash
# Fast unit tests only — no Docker required
./mvnw test

# Unit tests + Testcontainers-backed integration tests (requires Docker running)
./mvnw verify
```

The integration test (`WorkflowInstanceIT`) spins up real Postgres, Kafka, and Redis containers via Testcontainers, boots the full Spring context, and exercises the actual REST API — not mocks.

---

## Observability

- **Kafka UI** — `http://localhost:8090` — inspect topics, partitions, and individual messages for `workflow.task.dispatch`, `workflow.task.result`, `workflow.compensation.dispatch`, `workflow.compensation.result`, and their `.dlt` dead-letter counterparts.
- **Actuator** — `http://localhost:8080/actuator/health` and `/actuator/metrics` are exposed.
- **`GET /api/v1/workflow-instances/{id}`** — the primary tool for debugging a specific run: current status, current step index, failure reason if any, and the full per-step attempt history.

---

## Scope and Limitations

This is a **linear saga executor**: a fixed, ordered list of steps with retry-then-compensate semantics. It is well suited to coordinating a handful of services with compensating rollbacks, and every feature described above has been verified against real infrastructure, not just unit-tested in isolation.

It is **not** a substitute for a full workflow platform (e.g. Temporal) when the problem requires arbitrary branching logic, long-running human-in-the-loop approvals, or deterministic replay guarantees — those require a fundamentally different execution model built around replaying arbitrary code rather than advancing a state machine row by row.
