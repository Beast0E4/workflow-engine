package com.enginecorp.workfloworchestrator.exception;

import java.util.UUID;

public class WorkflowInstanceNotFoundException extends RuntimeException {

    public WorkflowInstanceNotFoundException(UUID workflowInstanceId) {
        super("No workflow instance found for id: " + workflowInstanceId);
    }
}