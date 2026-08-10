package com.enginecorp.workfloworchestrator.dto.response;

import com.enginecorp.workfloworchestrator.model.StepStatus;
import java.time.Instant;
import java.util.UUID;

public record StepExecutionResponse(
    UUID id,
    String taskName,
    StepStatus status,
    Integer attemptCount,
    String lastError,
    Instant startedAt,
    Instant completedAt
) {
}