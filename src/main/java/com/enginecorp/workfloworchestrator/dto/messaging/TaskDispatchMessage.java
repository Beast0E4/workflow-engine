package com.enginecorp.workfloworchestrator.dto.messaging;

import java.util.Map;
import java.util.UUID;

public record TaskDispatchMessage(
    UUID workflowInstanceId,
    UUID stepExecutionId,
    String taskName,
    Integer attemptNumber,
    Map<String, Object> payload
) {
}