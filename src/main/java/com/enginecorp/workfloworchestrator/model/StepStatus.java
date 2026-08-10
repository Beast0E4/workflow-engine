package com.enginecorp.workfloworchestrator.model;

public enum StepStatus {
    PENDING,
    DISPATCHED,
    RUNNING,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    SKIPPED
}