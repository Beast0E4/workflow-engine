package com.enginecorp.workfloworchestrator.repository;

import com.enginecorp.workfloworchestrator.model.StepExecution;
import java.time.Instant;
import java.util.List;

public interface StepExecutionRepositoryCustom {

    List<StepExecution> findEligibleForRetry(Instant cutoff);
}