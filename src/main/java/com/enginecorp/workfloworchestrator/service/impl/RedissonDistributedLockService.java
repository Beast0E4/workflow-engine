package com.enginecorp.workfloworchestrator.service.impl;

import com.enginecorp.workfloworchestrator.exception.DistributedLockAcquisitionException;
import com.enginecorp.workfloworchestrator.service.DistributedLockService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RedissonDistributedLockService implements DistributedLockService {

    private static final Logger logger =
            LoggerFactory.getLogger(RedissonDistributedLockService.class);

    private static final String LOCK_KEY_PREFIX = "workflow-engine:lock:";

    private final RedissonClient redissonClient;
    private final long waitTimeSeconds;
    private final long leaseTimeSeconds;

    public RedissonDistributedLockService(
            RedissonClient redissonClient,
            @Value("${workflow-engine.lock.wait-time-seconds}") long waitTimeSeconds,
            @Value("${workflow-engine.lock.lease-time-seconds}") long leaseTimeSeconds) {

        this.redissonClient = redissonClient;
        this.waitTimeSeconds = waitTimeSeconds;
        this.leaseTimeSeconds = leaseTimeSeconds;
    }

    @Override
    public <T> T executeWithLock(String lockKey, Supplier<T> action) {
        RLock lock = redissonClient.getLock(LOCK_KEY_PREFIX + lockKey);

        tryAcquire(lock, lockKey);

        try {
            return action.get();
        } finally {
            releaseIfHeld(lock);
        }
    }

    @Override
    public void executeWithLock(String lockKey, Runnable action) {
        executeWithLock(lockKey, () -> {
            action.run();
            return null;
        });
    }

    private boolean tryAcquire(RLock lock, String lockKey) {
        try {
            boolean acquired =
                    lock.tryLock(waitTimeSeconds, leaseTimeSeconds, TimeUnit.SECONDS);

            if (!acquired) {
                throw new DistributedLockAcquisitionException(lockKey);
            }

            return true;
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new DistributedLockAcquisitionException(lockKey);
        }
    }

    private void releaseIfHeld(RLock lock) {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        } else {
            logger.debug(
                    "Skipping unlock for {} — lock not held by current thread",
                    lock.getName()
            );
        }
    }
}