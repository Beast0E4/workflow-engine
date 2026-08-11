package com.enginecorp.workfloworchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowDefinitionRequest;
import com.enginecorp.workfloworchestrator.dto.request.WorkflowStartRequest;
import com.enginecorp.workfloworchestrator.dto.request.WorkflowStepRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowDefinitionResponse;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowInstanceResponse;
import com.enginecorp.workfloworchestrator.model.StepStatus;
import com.enginecorp.workfloworchestrator.model.WorkflowStatus;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowInstanceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("workflow_engine")
        .withUsername("workflow_user")
        .withPassword("workflow_pass");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
        .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("redisson.address", () -> "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());
    }

    @LocalServerPort
    private int localServerPort;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    void startingAWorkflowPersistsAnInstanceAndDispatchesTheFirstStep() {
        WorkflowDefinitionRequest definitionRequest = new WorkflowDefinitionRequest(
            "order-fulfillment",
            "Reserve inventory then charge payment",
            List.of(
                new WorkflowStepRequest("reserve-inventory", "release-inventory", "inventory.tasks", 2, 15000L),
                new WorkflowStepRequest("charge-payment", "refund-payment", "payment.tasks", 2, 15000L)
            )
        );

        WorkflowDefinitionResponse definitionResponse = testRestTemplate.postForObject(
            "/api/v1/workflow-definitions",
            definitionRequest,
            WorkflowDefinitionResponse.class
        );

        assertThat(definitionResponse).isNotNull();
        assertThat(definitionResponse.steps()).hasSize(2);

        WorkflowStartRequest startRequest = new WorkflowStartRequest(
            definitionResponse.id(),
            Map.of("orderId", "ORD-1001")
        );

        WorkflowInstanceResponse instanceResponse = testRestTemplate.postForObject(
            "/api/v1/workflow-instances",
            startRequest,
            WorkflowInstanceResponse.class
        );

        assertThat(instanceResponse).isNotNull();
        assertThat(instanceResponse.status()).isEqualTo(WorkflowStatus.RUNNING);
        assertThat(instanceResponse.currentStepIndex()).isZero();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            WorkflowInstanceResponse refreshed = testRestTemplate.getForObject(
                "/api/v1/workflow-instances/" + instanceResponse.id(),
                WorkflowInstanceResponse.class
            );
            assertThat(refreshed.stepExecutions().get(0).status()).isEqualTo(StepStatus.DISPATCHED);
        });
    }
}