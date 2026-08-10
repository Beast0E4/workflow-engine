package com.enginecorp.workfloworchestrator.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record WorkflowStepRequest(
    @NotBlank(message = "Task name must not be blank") String taskName,
    String compensationTaskName,
    @NotBlank(message = "Target topic must not be blank") String targetTopic,
    @Min(value = 0, message = "Retry limit cannot be negative") Integer retryLimit,
    @Positive(message = "Timeout must be a positive number of milliseconds") Long timeoutMs
) {
}