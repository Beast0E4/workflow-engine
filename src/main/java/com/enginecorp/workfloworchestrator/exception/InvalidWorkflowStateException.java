package com.enginecorp.workfloworchestrator.exception;

public class InvalidWorkflowStateException extends RuntimeException {

    public InvalidWorkflowStateException(String message) {
        super(message);
    }
}