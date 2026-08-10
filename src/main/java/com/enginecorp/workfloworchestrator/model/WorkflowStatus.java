package com.enginecorp.workfloworchestrator.model;

public enum WorkflowStatus {
    CREATED,
    RUNNING,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED
}