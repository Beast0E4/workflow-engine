# Workflow Orchestrator

A distributed workflow orchestration engine built with Spring Boot, PostgreSQL, Kafka, and Redis/Redisson.

The service enables definition and execution of multi-step workflows, supports asynchronous task execution through Kafka, automatic retries, distributed locking, and Saga-style compensation for failed workflows.

---

## Features

- Workflow definition management
- Versioned workflow definitions
- Multi-step workflow execution
- Kafka-based asynchronous task dispatch
- Distributed workflow coordination
- Retry scheduling with exponential backoff
- Saga compensation support
- Optimistic locking for concurrency control
- Distributed locking with Redisson
- PostgreSQL persistence
- Flyway database migrations
- REST APIs for workflow management
- Health, metrics, and actuator endpoints

---

## Architecture

```text
                   +-------------------+
                   |   REST Clients    |
                   +---------+---------+
                             |
                             v
                  +----------------------+
                  | Workflow Orchestrator|
                  +----------+-----------+
                             |
             +---------------+----------------+
             |                                |
             v                                v
     +---------------+              +----------------+
     | PostgreSQL    |              | Redis/Redisson |
     | Persistence   |              | Distributed    |
     |               |              | Locks          |
     +---------------+              +----------------+
             |
             v
     +------------------+
     | Workflow State   |
     | & Definitions    |
     +------------------+

             |
             v

     +------------------+
     | Kafka Topics     |
     +------------------+
             |
    +--------+---------+
    |                  |
    v                  v

Task Workers    Compensation Workers
    |                  |
    +--------+---------+
             |
             v
      Result Topics
             |
             v
Workflow Orchestrator
```

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Language | Java 18 |
| Framework | Spring Boot |
| Database | PostgreSQL |
| Messaging | Apache Kafka |
| Distributed Locking | Redis + Redisson |
| ORM | Spring Data JPA / Hibernate |
| Migrations | Flyway |
| Build Tool | Maven |
| Validation | Jakarta Validation |
| Monitoring | Spring Actuator |

---

## Project Structure

```text
src/main/java/com/enginecorp/workfloworchestrator

├── config
│   ├── AsyncExecutorConfig
│   ├── JpaAuditingConfig
│   ├── KafkaConsumerConfig
│   ├── KafkaProducerConfig
│   ├── KafkaTopicConfig
│   └── RedissonConfig
│
├── controller
│   ├── WorkflowDefinitionController
│   └── WorkflowInstanceController
│
├── dto
│   ├── messaging
│   ├── request
│   └── response
│
├── exception
│
├── model
│
├── repository
│
├── service
│   └── impl
│
└── worker
    ├── TaskResultListener
    ├── CompensationResultListener
    └── StepRetryScheduler
```

---

## Database Schema

### Workflow Definition

Represents a reusable workflow template.

```text
WorkflowDefinition
 ├── id
 ├── name
 ├── version
 ├── description
 ├── active
 └── steps
```

### Workflow Step Definition

Represents a step inside a workflow.

```text
WorkflowStepDefinition
 ├── stepOrder
 ├── taskName
 ├── compensationTaskName
 ├── retryLimit
 ├── timeoutMs
 └── targetTopic
```

### Workflow Instance

Represents a running workflow execution.

```text
WorkflowInstance
 ├── id
 ├── status
 ├── currentStepIndex
 ├── payload
 ├── failureReason
 └── stepExecutions
```

### Step Execution

Tracks execution state of an individual workflow step.

```text
StepExecution
 ├── status
 ├── attemptCount
 ├── lastError
 ├── nextRetryAt
 ├── startedAt
 └── completedAt
```

---

## Workflow Lifecycle

### Workflow States

```text
CREATED
   |
   v
RUNNING
   |
   +----------------+
   |                |
   v                v
COMPLETED       FAILED
                    |
                    v
             COMPENSATING
                    |
                    v
             COMPENSATED
```

### Step States

```text
PENDING
   |
   v
DISPATCHED
   |
   v
RUNNING
   |
   +----------------+
   |                |
   v                v
COMPLETED        FAILED
                    |
                    +------------+
                    |            |
                    v            v
              RETRY         COMPENSATING
                                  |
                                  v
                            COMPENSATED
```

---

## Kafka Topics

| Topic | Purpose |
|---------|---------|
| workflow.task.dispatch | Dispatch workflow tasks |
| workflow.task.result | Receive task execution results |
| workflow.compensation.dispatch | Dispatch compensation tasks |
| workflow.compensation.result | Receive compensation results |

Default configuration:

```yaml
workflow-engine:
  kafka:
    topics:
      task-dispatch: workflow.task.dispatch
      task-result: workflow.task.result
      compensation-dispatch: workflow.compensation.dispatch
      compensation-result: workflow.compensation.result
      partitions: 6
      replication-factor: 1
```

---

## Retry Strategy

Failed steps are automatically retried.

Configuration:

```yaml
workflow-engine:
  retry:
    default-max-attempts: 3
    backoff-initial-ms: 2000
    backoff-multiplier: 2.0
    scheduler-fixed-delay-ms: 5000
```

Example retry schedule:

```text
Attempt 1 -> Failure

Wait 2 seconds

Attempt 2 -> Failure

Wait 4 seconds

Attempt 3 -> Failure

Workflow Failure
```

---

## Compensation (Saga Pattern)

When a workflow step exceeds retry limits:

1. Workflow enters FAILED state
2. Workflow enters COMPENSATING state
3. Completed steps are compensated in reverse order
4. Compensation messages are sent through Kafka
5. Workflow becomes COMPENSATED

Example:

```text
Create Order        ✓
Reserve Inventory   ✓
Process Payment     ✗

Compensation Begins

Reserve Inventory Compensation
Create Order Compensation

Workflow -> COMPENSATED
```

---

## Configuration

### Application Configuration

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/workflow_engine
    username: workflow_user
    password: workflow_pass

  kafka:
    bootstrap-servers: localhost:9092

redisson:
  address: redis://localhost:6379
  database: 0
```

---

## Running Locally

### Prerequisites

- Java 18+
- Maven 3.9+
- PostgreSQL
- Apache Kafka
- Redis

### Build

```bash
mvn clean compile
```

### Run Tests

```bash
mvn test
```

### Start Application

```bash
mvn spring-boot:run
```

Or

```bash
java -jar target/workflow-orchestrator.jar
```

---

## API Overview

### Create Workflow Definition

```http
POST /api/workflows/definitions
```

Example:

```json
{
  "name": "order-processing",
  "description": "Order processing workflow",
  "steps": [
    {
      "taskName": "create-order",
      "targetTopic": "order-service",
      "retryLimit": 3,
      "timeoutMs": 30000
    },
    {
      "taskName": "reserve-inventory",
      "compensationTaskName": "release-inventory",
      "targetTopic": "inventory-service",
      "retryLimit": 3,
      "timeoutMs": 30000
    }
  ]
}
```

---

### Start Workflow

```http
POST /api/workflows/start
```

Example:

```json
{
  "workflowDefinitionId": "workflow-uuid",
  "payload": {
    "orderId": "12345",
    "customerId": "cust-001"
  }
}
```

---

### Get Workflow Instance

```http
GET /api/workflows/{workflowInstanceId}
```

---

## Monitoring

Spring Boot Actuator endpoints:

```text
/actuator/health
/actuator/info
/actuator/metrics
```

---

## Concurrency and Consistency

The engine uses:

- Optimistic locking via `@Version`
- Distributed locking via Redisson
- Kafka idempotent producers
- Manual Kafka acknowledgements
- Transactional state transitions

This prevents duplicate workflow progression in clustered deployments.

---

## Future Improvements

- Workflow visualization UI
- Workflow version deprecation
- Dead-letter queue support
- OpenTelemetry tracing
- Dynamic retry policies
- Workflow pause/resume
- Scheduled workflow execution
- Multi-tenant support
- Workflow state machine framework integration

---