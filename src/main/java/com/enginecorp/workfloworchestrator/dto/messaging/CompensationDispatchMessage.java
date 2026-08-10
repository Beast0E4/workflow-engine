package com.enginecorp.workfloworchestrator.dto.messaging;

import java.util.Map;
import java.util.UUID;

public record CompensationDispatchMessage(
    UUID workflowInstanceId,
    UUID stepExecutionId,
    String compensationTaskName,
    Map<String, Object> payload
) {
}