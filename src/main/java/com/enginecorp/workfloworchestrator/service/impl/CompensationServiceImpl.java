package com.enginecorp.workfloworchestrator.service.impl;

import com.enginecorp.workfloworchestrator.dto.messaging.CompensationDispatchMessage;
import com.enginecorp.workfloworchestrator.dto.messaging.CompensationResultMessage;
import com.enginecorp.workfloworchestrator.exception.WorkflowInstanceNotFoundException;
import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.model.StepStatus;
import com.enginecorp.workfloworchestrator.model.WorkflowInstance;
import com.enginecorp.workfloworchestrator.model.WorkflowStatus;
import com.enginecorp.workfloworchestrator.repository.WorkflowInstanceRepository;
import com.enginecorp.workfloworchestrator.service.CompensationService;
import com.enginecorp.workfloworchestrator.service.DistributedLockService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CompensationServiceImpl implements CompensationService {

    private static final Logger logger = LoggerFactory.getLogger(CompensationServiceImpl.class);
    private static final String INSTANCE_LOCK_PREFIX = "workflow-instance:";

    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final KafkaTemplate<String, Object> workflowKafkaTemplate;
    private final DistributedLockService distributedLockService;
    private final ObjectMapper objectMapper;
    private final String compensationDispatchTopic;

    public CompensationServiceImpl(
        WorkflowInstanceRepository workflowInstanceRepository,
        KafkaTemplate<String, Object> workflowKafkaTemplate,
        DistributedLockService distributedLockService,
        ObjectMapper objectMapper,
        @Value("${workflow-engine.kafka.topics.compensation-dispatch}") String compensationDispatchTopic
    ) {
        this.workflowInstanceRepository = workflowInstanceRepository;
        this.workflowKafkaTemplate = workflowKafkaTemplate;
        this.distributedLockService = distributedLockService;
        this.objectMapper = objectMapper;
        this.compensationDispatchTopic = compensationDispatchTopic;
    }

    @Override
    public void beginCompensation(UUID workflowInstanceId) {
        distributedLockService.executeWithLock(INSTANCE_LOCK_PREFIX + workflowInstanceId, () -> {
            beginCompensationLocked(workflowInstanceId);
        });
    }

    @Transactional
    protected void beginCompensationLocked(UUID workflowInstanceId) {
        WorkflowInstance instance = workflowInstanceRepository.findByIdForUpdate(workflowInstanceId)
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(workflowInstanceId));

        List<StepExecution> completedStepsInReverseOrder = instance.getStepExecutions()
            .stream()
            .filter(step -> step.getStatus() == StepStatus.COMPLETED)
            .sorted((first, second) -> second.getStepDefinition().getStepOrder() - first.getStepDefinition().getStepOrder())
            .toList();

        Optional<StepExecution> firstCompensableStep = completedStepsInReverseOrder.stream()
            .filter(step -> step.getStepDefinition().getCompensationTaskName() != null)
            .findFirst();

        if (firstCompensableStep.isEmpty()) {
            instance.setStatus(WorkflowStatus.COMPENSATED);
            workflowInstanceRepository.save(instance);
            logger.info("Workflow instance {} had no compensable steps, marked as COMPENSATED", workflowInstanceId);
            return;
        }

        StepExecution stepToCompensate = firstCompensableStep.get();
        dispatchCompensation(instance, stepToCompensate);
        workflowInstanceRepository.save(instance);
    }

    private void dispatchCompensation(WorkflowInstance instance, StepExecution stepExecution) {
        stepExecution.setStatus(StepStatus.COMPENSATING);

        CompensationDispatchMessage dispatchMessage = new CompensationDispatchMessage(
            instance.getId(),
            stepExecution.getId(),
            stepExecution.getStepDefinition().getCompensationTaskName(),
            deserializePayload(instance.getPayload())
        );

        workflowKafkaTemplate.send(compensationDispatchTopic, instance.getId().toString(), dispatchMessage);
        logger.info(
            "Dispatched compensation '{}' for workflow instance {}",
            stepExecution.getStepDefinition().getCompensationTaskName(),
            instance.getId()
        );
    }

    @Override
    public void handleCompensationResult(CompensationResultMessage message) {
        distributedLockService.executeWithLock(INSTANCE_LOCK_PREFIX + message.workflowInstanceId(), () -> {
            handleCompensationResultLocked(message);
        });
    }

    @Transactional
    protected void handleCompensationResultLocked(CompensationResultMessage message) {
        WorkflowInstance instance = workflowInstanceRepository.findByIdForUpdate(message.workflowInstanceId())
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(message.workflowInstanceId()));

        StepExecution compensatedStep = instance.getStepExecutions()
            .stream()
            .filter(step -> step.getId().equals(message.stepExecutionId()))
            .findFirst()
            .orElseThrow(() -> new WorkflowInstanceNotFoundException(message.workflowInstanceId()));

        if (compensatedStep.getStatus() != StepStatus.COMPENSATING) {
            logger.debug(
                "Ignoring compensation result for step {} in state {} — expected COMPENSATING, likely a duplicate delivery",
                compensatedStep.getId(),
                compensatedStep.getStatus()
            );
            return;
        }

        if (!message.successful()) {
            compensatedStep.setLastError(message.errorMessage());
            logger.error(
                "Compensation failed for step {} on workflow instance {}: {} — manual intervention required",
                compensatedStep.getStepDefinition().getTaskName(),
                message.workflowInstanceId(),
                message.errorMessage()
            );
            workflowInstanceRepository.save(instance);
            return;
        }

        compensatedStep.setStatus(StepStatus.COMPENSATED);

        List<StepExecution> remainingCompensableSteps = instance.getStepExecutions()
            .stream()
            .filter(step -> step.getStatus() == StepStatus.COMPLETED)
            .filter(step -> step.getStepDefinition().getCompensationTaskName() != null)
            .sorted((first, second) -> second.getStepDefinition().getStepOrder() - first.getStepDefinition().getStepOrder())
            .toList();

        if (remainingCompensableSteps.isEmpty()) {
            instance.setStatus(WorkflowStatus.COMPENSATED);
            logger.info("Workflow instance {} fully compensated", message.workflowInstanceId());
        } else {
            dispatchCompensation(instance, remainingCompensableSteps.get(0));
        }

        workflowInstanceRepository.save(instance);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializePayload(String payload) {
        try {
            return payload != null ? objectMapper.readValue(payload, Map.class) : Collections.emptyMap();
        } catch (Exception deserializationException) {
            logger.warn("Failed to deserialize payload during compensation dispatch: {}", deserializationException.getMessage());
            return Collections.emptyMap();
        }
    }
}