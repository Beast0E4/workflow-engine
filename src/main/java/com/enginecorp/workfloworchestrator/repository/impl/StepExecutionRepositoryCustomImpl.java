package com.enginecorp.workfloworchestrator.repository.impl;

import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.model.StepStatus;
import com.enginecorp.workfloworchestrator.repository.StepExecutionRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class StepExecutionRepositoryCustomImpl implements StepExecutionRepositoryCustom {

    private final EntityManager entityManager;

    public StepExecutionRepositoryCustomImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public List<StepExecution> findEligibleForRetry(Instant cutoff) {
        TypedQuery<StepExecution> query = entityManager.createQuery(
            "select stepExecution from StepExecution stepExecution "
                + "where stepExecution.status = :failedStatus "
                + "and stepExecution.nextRetryAt is not null "
                + "and stepExecution.nextRetryAt <= :cutoff",
            StepExecution.class
        );
        query.setParameter("failedStatus", StepStatus.FAILED);
        query.setParameter("cutoff", cutoff);
        return query.getResultList();
    }
}