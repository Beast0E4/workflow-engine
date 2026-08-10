package com.enginecorp.workfloworchestrator.dto.request;

import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;

public record WorkflowStartRequest(
    @NotNull(message = "Workflow definition id is required") UUID workflowDefinitionId,
    Map<String, Object> payload
) {
}