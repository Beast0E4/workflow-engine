package com.enginecorp.workfloworchestrator.dto.messaging;

import java.util.UUID;

public record CompensationResultMessage(
    UUID workflowInstanceId,
    UUID stepExecutionId,
    boolean successful,
    String errorMessage
) {
}