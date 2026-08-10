package com.enginecorp.workfloworchestrator.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record WorkflowDefinitionRequest(
    @NotBlank(message = "Workflow name must not be blank") String name,
    String description,
    @NotEmpty(message = "A workflow must declare at least one step")
    @Valid
    List<WorkflowStepRequest> steps
) {
}