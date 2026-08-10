package com.enginecorp.workfloworchestrator.service;

import java.util.function.Supplier;

public interface DistributedLockService {

    <T> T executeWithLock(String lockKey, Supplier<T> action);

    void executeWithLock(String lockKey, Runnable action);
}