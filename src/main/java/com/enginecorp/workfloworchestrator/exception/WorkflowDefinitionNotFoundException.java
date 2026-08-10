package com.enginecorp.workfloworchestrator.exception;

import java.util.UUID;

public class WorkflowDefinitionNotFoundException extends RuntimeException {

    public WorkflowDefinitionNotFoundException(UUID workflowDefinitionId) {
        super("No active workflow definition found for id: " + workflowDefinitionId);
    }
}