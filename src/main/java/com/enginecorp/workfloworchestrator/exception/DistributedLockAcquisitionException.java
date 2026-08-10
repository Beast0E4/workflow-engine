package com.enginecorp.workfloworchestrator.exception;

public class DistributedLockAcquisitionException extends RuntimeException {

    public DistributedLockAcquisitionException(String lockKey) {
        super("Failed to acquire distributed lock for key: " + lockKey);
    }
}