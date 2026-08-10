package com.enginecorp.workfloworchestrator.dto.messaging;

import java.util.Map;
import java.util.UUID;

public record TaskResultMessage(
    UUID workflowInstanceId,
    UUID stepExecutionId,
    boolean successful,
    String errorMessage,
    Map<String, Object> resultPayload
) {
}