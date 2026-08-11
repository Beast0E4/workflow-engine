package com.enginecorp.workfloworchestrator.service.impl;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowStartRequest;
import com.enginecorp.workfloworchestrator.dto.response.StepExecutionResponse;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowInstanceResponse;
import com.enginecorp.workfloworchestrator.exception.InvalidWorkflowStateException;
import com.enginecorp.workfloworchestrator.exception.WorkflowDefinitionNotFoundException;
import com.enginecorp.workfloworchestrator.exception.WorkflowInstanceNotFoundException;
import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.model.StepStatus;
import com.enginecorp.workfloworchestrator.model.WorkflowDefinition;
import com.enginecorp.workfloworchestrator.model.WorkflowInstance;
import com.enginecorp.workfloworchestrator.model.WorkflowStatus;
import com.enginecorp.workfloworchestrator.model.WorkflowStepDefinition;
import com.enginecorp.workfloworchestrator.repository.WorkflowDefinitionRepository;
import com.enginecorp.workfloworchestrator.repository.WorkflowInstanceRepository;
import com.enginecorp.workfloworchestrator.service.CompensationService;
import com.enginecorp.workfloworchestrator.service.DistributedLockService;
import com.enginecorp.workfloworchestrator.service.StepDispatchService;
import com.enginecorp.workfloworchestrator.service.WorkflowOrchestrationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WorkflowOrchestrationServiceImpl implements WorkflowOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowOrchestrationServiceImpl.class);
    private static final String INSTANCE_LOCK_PREFIX = "workflow-instance:";

    private final WorkflowDefinitionRepository workflowDefinitionRepository;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final StepDispatchService stepDispatchService;
    private final CompensationService compensationService;
    private final DistributedLockService distributedLockService;
    private final ObjectMapper objectMapper;

    public WorkflowOrchestrationServiceImpl(
        WorkflowDefinitionRepository workflowDefinitionRepository,
        WorkflowInstanceRepository workflowInstanceRepository,
        StepDispatchService stepDispatchService,
        CompensationService compensationService,
        DistributedLockService distributedLockService,
        ObjectMapper objectMapper
    ) {
        this.workflowDefinitionRepository = workflowDefinitionRepository;
        this.workflowInstanceRepository = workflowInstanceRepository;
        this.stepDispatchService = stepDispatchService;
        this.compensationService = compensationService;
        this.distributedLockService = distributedLockService;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public WorkflowInstanceResponse startWorkflow(WorkflowStartRequest request) {
        WorkflowDefinition definition = workflowDefinitionRepository.findByIdAndActiveTrue(request.workflowDefinitionId())
            .orElseThrow(() -> new WorkflowDefinitionNotFoundException(request.workflowDefinitionId()));

        if (definition.getSteps().isEmpty()) {
            throw new InvalidWorkflowStateException(
                "Workflow definition has no steps to execute: " + definition.getId()
            );
        }

        WorkflowInstance instance = new WorkflowInstance();
        instance.setWorkflowDefinition(definition);
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStepIndex(0);
        instance.setPayload(serializePayload(request.payload()));

        for (WorkflowStepDefinition stepDefinition : definition.getSteps()) {
            StepExecution stepExecution = new StepExecution();
            stepExecution.setStepDefinition(stepDefinition);
            stepExecution.setStatus(StepStatus.PENDING);
            instance.addStepExecution(stepExecution);
        }

        WorkflowInstance persistedInstance = workflowInstanceRepository.save(instance);
        dispatchStepAtIndex(persistedInstance, 0);

        return toResponse(workflowInstanceRepository.save(persistedInstance));
    }

    @Override
    @Transactional(readOnly = true)
    public WorkflowInstanceResponse getInstance(UUID workflowInstanceId) {
        WorkflowInstance instance = workflowInstanceRepository.findById(workflowInstanceId)
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(workflowInstanceId));

        return toResponse(instance);
    }

    @Override
    public void advanceAfterStepSuccess(UUID workflowInstanceId, UUID stepExecutionId) {
        distributedLockService.executeWithLock(
            INSTANCE_LOCK_PREFIX + workflowInstanceId,
            () -> advanceAfterStepSuccessLocked(workflowInstanceId, stepExecutionId)
        );
    }

    @Transactional
    protected void advanceAfterStepSuccessLocked(UUID workflowInstanceId, UUID stepExecutionId) {
        WorkflowInstance instance = workflowInstanceRepository.findByIdForUpdate(workflowInstanceId)
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(workflowInstanceId));

        StepExecution completedStep = findStepExecution(instance, stepExecutionId);

        if (completedStep.getStatus() == StepStatus.COMPLETED) {
            logger.debug("Ignoring duplicate success result for step {} — already COMPLETED", stepExecutionId);
            return;
        }

        if (completedStep.getStatus() != StepStatus.DISPATCHED) {
            logger.warn(
                "Ignoring success result for step {} in unexpected state {} — expected DISPATCHED",
                stepExecutionId,
                completedStep.getStatus()
            );
            return;
        }

        completedStep.setStatus(StepStatus.COMPLETED);
        completedStep.setCompletedAt(Instant.now());

        int nextStepIndex = instance.getCurrentStepIndex() + 1;
        instance.setCurrentStepIndex(nextStepIndex);

        int totalSteps = instance.getWorkflowDefinition().getSteps().size();
        if (nextStepIndex >= totalSteps) {
            instance.setStatus(WorkflowStatus.COMPLETED);
            logger.info("Workflow instance {} completed all {} steps", workflowInstanceId, totalSteps);
        } else {
            dispatchStepAtIndex(instance, nextStepIndex);
        }

        workflowInstanceRepository.save(instance);
    }

    @Override
    public void handleStepFailure(
        UUID workflowInstanceId,
        UUID stepExecutionId,
        String errorMessage
    ) {
        distributedLockService.executeWithLock(
            INSTANCE_LOCK_PREFIX + workflowInstanceId,
            () -> handleStepFailureLocked(workflowInstanceId, stepExecutionId, errorMessage)
        );
    }

    @Transactional
    protected void handleStepFailureLocked(UUID workflowInstanceId, UUID stepExecutionId, String errorMessage) {
        WorkflowInstance instance = workflowInstanceRepository.findByIdForUpdate(workflowInstanceId)
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(workflowInstanceId));

        StepExecution failedStep = findStepExecution(instance, stepExecutionId);

        if (failedStep.getStatus() != StepStatus.DISPATCHED) {
            logger.debug(
                "Ignoring failure result for step {} in state {} — expected DISPATCHED, likely a duplicate delivery",
                stepExecutionId,
                failedStep.getStatus()
            );
            return;
        }

        failedStep.setAttemptCount(failedStep.getAttemptCount() + 1);
        failedStep.setLastError(errorMessage);

        if (failedStep.hasExceededRetryLimit()) {
            failedStep.setStatus(StepStatus.FAILED);
            failedStep.setNextRetryAt(null);
            instance.setStatus(WorkflowStatus.COMPENSATING);
            instance.setFailureReason(errorMessage);
            workflowInstanceRepository.save(instance);
            compensationService.beginCompensation(workflowInstanceId);
            logger.warn(
                "Step {} exhausted retry budget for workflow instance {}, entering compensation",
                failedStep.getStepDefinition().getTaskName(),
                workflowInstanceId
            );
        } else {
            failedStep.setStatus(StepStatus.FAILED);
            failedStep.setNextRetryAt(computeNextRetryTime(failedStep.getAttemptCount()));
            workflowInstanceRepository.save(instance);
            logger.info(
                "Step {} scheduled for retry attempt {} at {}",
                failedStep.getStepDefinition().getTaskName(),
                failedStep.getAttemptCount() + 1,
                failedStep.getNextRetryAt()
            );
        }
    }

    @Override
    public void retryEligibleStep(UUID workflowInstanceId, UUID stepExecutionId) {
        distributedLockService.executeWithLock(
            INSTANCE_LOCK_PREFIX + workflowInstanceId,
            () -> retryEligibleStepLocked(workflowInstanceId, stepExecutionId)
        );
    }

    @Transactional
    protected void retryEligibleStepLocked(UUID workflowInstanceId, UUID stepExecutionId) {
        WorkflowInstance instance = workflowInstanceRepository.findByIdForUpdate(workflowInstanceId)
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(workflowInstanceId));

        StepExecution stepExecution = findStepExecution(instance, stepExecutionId);

        if (stepExecution.getStatus() != StepStatus.FAILED) {
            logger.debug(
                "Skipping retry for step {} — no longer in FAILED state",
                stepExecutionId
            );
            return;
        }

        stepExecution.setNextRetryAt(null);
        stepDispatchService.dispatchStep(
            instance,
            stepExecution,
            stepExecution.getAttemptCount() + 1
        );

        workflowInstanceRepository.save(instance);
    }

    private void dispatchStepAtIndex(WorkflowInstance instance, int stepIndex) {
        WorkflowStepDefinition stepDefinition =
            instance.getWorkflowDefinition().getSteps().get(stepIndex);

        StepExecution stepExecution = instance.getStepExecutions()
            .stream()
            .filter(candidate ->
                candidate.getStepDefinition().getId().equals(stepDefinition.getId())
            )
            .findFirst()
            .orElseThrow(() ->
                new InvalidWorkflowStateException(
                    "Missing step execution for step definition: " + stepDefinition.getId()
                )
            );

        stepDispatchService.dispatchStep(instance, stepExecution, 1);
    }

    private StepExecution findStepExecution(
        WorkflowInstance instance,
        UUID stepExecutionId
    ) {
        return instance.getStepExecutions()
            .stream()
            .filter(candidate -> candidate.getId().equals(stepExecutionId))
            .findFirst()
            .orElseThrow(() ->
                new InvalidWorkflowStateException(
                    "Step execution not found on instance: " + stepExecutionId
                )
            );
    }

    private Instant computeNextRetryTime(int attemptCount) {
        long backoffMillis = (long) (2000 * Math.pow(2.0, attemptCount - 1));
        return Instant.now().plusMillis(backoffMillis);
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(
                payload != null ? payload : Map.of()
            );
        } catch (Exception serializationException) {
            throw new InvalidWorkflowStateException(
                "Unable to serialize workflow payload: "
                    + serializationException.getMessage()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializePayload(String payload) {
        try {
            return payload != null
                ? objectMapper.readValue(payload, Map.class)
                : Map.of();
        } catch (Exception deserializationException) {
            logger.warn(
                "Failed to deserialize instance payload, returning empty map: {}",
                deserializationException.getMessage()
            );
            return Map.of();
        }
    }

    private WorkflowInstanceResponse toResponse(WorkflowInstance instance) {
        List<StepExecutionResponse> stepResponses = instance.getStepExecutions()
            .stream()
            .map(step -> new StepExecutionResponse(
                step.getId(),
                step.getStepDefinition().getTaskName(),
                step.getStatus(),
                step.getAttemptCount(),
                step.getLastError(),
                step.getStartedAt(),
                step.getCompletedAt()
            ))
            .toList();

        return new WorkflowInstanceResponse(
            instance.getId(),
            instance.getWorkflowDefinition().getId(),
            instance.getWorkflowDefinition().getName(),
            instance.getStatus(),
            deserializePayload(instance.getPayload()),
            instance.getCurrentStepIndex(),
            instance.getFailureReason(),
            stepResponses,
            instance.getCreatedAt(),
            instance.getUpdatedAt()
        );
    }
}