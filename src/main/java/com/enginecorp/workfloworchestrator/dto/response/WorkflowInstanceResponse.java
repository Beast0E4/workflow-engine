package com.enginecorp.workfloworchestrator.dto.response;

import com.enginecorp.workfloworchestrator.model.WorkflowStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowInstanceResponse(
    UUID id,
    UUID workflowDefinitionId,
    String workflowDefinitionName,
    WorkflowStatus status,
    Integer currentStepIndex,
    String failureReason,
    List<StepExecutionResponse> stepExecutions,
    Instant createdAt,
    Instant updatedAt
) {
}