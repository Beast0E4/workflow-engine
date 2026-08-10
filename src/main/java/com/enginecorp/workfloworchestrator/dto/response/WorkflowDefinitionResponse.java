package com.enginecorp.workfloworchestrator.dto.response;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowDefinitionResponse(
    UUID id,
    String name,
    Integer version,
    String description,
    boolean active,
    List<WorkflowStepSummary> steps,
    Instant createdAt
) {
    public record WorkflowStepSummary(
        Integer stepOrder,
        String taskName,
        String compensationTaskName,
        String targetTopic,
        Integer retryLimit
    ) {
    }
}