package com.enginecorp.workfloworchestrator.worker;

import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.repository.StepExecutionRepository;
import com.enginecorp.workfloworchestrator.service.WorkflowOrchestrationService;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class StepRetryScheduler {

    private static final Logger logger = LoggerFactory.getLogger(StepRetryScheduler.class);

    private final StepExecutionRepository stepExecutionRepository;
    private final WorkflowOrchestrationService workflowOrchestrationService;

    public StepRetryScheduler(
        StepExecutionRepository stepExecutionRepository,
        WorkflowOrchestrationService workflowOrchestrationService
    ) {
        this.stepExecutionRepository = stepExecutionRepository;
        this.workflowOrchestrationService = workflowOrchestrationService;
    }

    @Scheduled(fixedDelayString = "${workflow-engine.retry.scheduler-fixed-delay-ms}")
    public void dispatchEligibleRetries() {
        List<StepExecution> eligibleSteps = stepExecutionRepository.findEligibleForRetry(Instant.now());
        if (eligibleSteps.isEmpty()) {
            return;
        }

        logger.info("Found {} step execution(s) eligible for retry", eligibleSteps.size());
        for (StepExecution stepExecution : eligibleSteps) {
            try {
                workflowOrchestrationService.retryEligibleStep(
                    stepExecution.getWorkflowInstance().getId(),
                    stepExecution.getId()
                );
            } catch (Exception retryDispatchException) {
                logger.error(
                    "Failed to dispatch retry for step execution {}",
                    stepExecution.getId(),
                    retryDispatchException
                );
            }
        }
    }
}