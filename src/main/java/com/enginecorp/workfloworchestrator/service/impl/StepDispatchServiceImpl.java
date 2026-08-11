package com.enginecorp.workfloworchestrator.service.impl;

import com.enginecorp.workfloworchestrator.dto.messaging.TaskDispatchMessage;
import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.model.StepStatus;
import com.enginecorp.workfloworchestrator.model.WorkflowInstance;
import com.enginecorp.workfloworchestrator.repository.StepExecutionRepository;
import com.enginecorp.workfloworchestrator.service.StepDispatchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StepDispatchServiceImpl implements StepDispatchService {

    private static final Logger logger = LoggerFactory.getLogger(StepDispatchServiceImpl.class);

    private final KafkaTemplate<String, Object> workflowKafkaTemplate;
    private final StepExecutionRepository stepExecutionRepository;
    private final ObjectMapper objectMapper;

    public StepDispatchServiceImpl(
        KafkaTemplate<String, Object> workflowKafkaTemplate,
        StepExecutionRepository stepExecutionRepository,
        ObjectMapper objectMapper
    ) {
        this.workflowKafkaTemplate = workflowKafkaTemplate;
        this.stepExecutionRepository = stepExecutionRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public void dispatchStep(WorkflowInstance workflowInstance, StepExecution stepExecution, int attemptNumber) {
        stepExecution.setStatus(StepStatus.DISPATCHED);
        stepExecution.setStartedAt(Instant.now());
        StepExecution persistedStep = stepExecutionRepository.save(stepExecution);

        TaskDispatchMessage dispatchMessage = new TaskDispatchMessage(
            workflowInstance.getId(),
            persistedStep.getId(),
            persistedStep.getStepDefinition().getTaskName(),
            attemptNumber,
            deserializeInstancePayload(workflowInstance.getPayload())
        );

        String targetTopic = persistedStep.getStepDefinition().getTargetTopic();
        workflowKafkaTemplate.send(targetTopic, workflowInstance.getId().toString(), dispatchMessage);
        logger.info(
            "Dispatched task '{}' (attempt {}) for workflow instance {} to topic {}",
            persistedStep.getStepDefinition().getTaskName(),
            attemptNumber,
            workflowInstance.getId(),
            targetTopic
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializeInstancePayload(String payload) {
        try {
            return payload != null ? objectMapper.readValue(payload, Map.class) : Map.of();
        } catch (Exception deserializationException) {
            logger.warn("Failed to deserialize payload for dispatch, sending empty payload: {}", deserializationException.getMessage());
            return Map.of();
        }
    }
}