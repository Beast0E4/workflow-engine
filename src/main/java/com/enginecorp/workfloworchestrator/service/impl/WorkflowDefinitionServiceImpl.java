package com.enginecorp.workfloworchestrator.service.impl;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowDefinitionRequest;
import com.enginecorp.workfloworchestrator.dto.request.WorkflowStepRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowDefinitionResponse;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowDefinitionResponse.WorkflowStepSummary;
import com.enginecorp.workfloworchestrator.exception.WorkflowDefinitionNotFoundException;
import com.enginecorp.workfloworchestrator.model.WorkflowDefinition;
import com.enginecorp.workfloworchestrator.model.WorkflowStepDefinition;
import com.enginecorp.workfloworchestrator.repository.WorkflowDefinitionRepository;
import com.enginecorp.workfloworchestrator.service.WorkflowDefinitionService;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WorkflowDefinitionServiceImpl implements WorkflowDefinitionService {

    private final WorkflowDefinitionRepository workflowDefinitionRepository;

    public WorkflowDefinitionServiceImpl(WorkflowDefinitionRepository workflowDefinitionRepository) {
        this.workflowDefinitionRepository = workflowDefinitionRepository;
    }

    @Override
    @Transactional
    public WorkflowDefinitionResponse createDefinition(WorkflowDefinitionRequest request) {
        int nextVersion = workflowDefinitionRepository.findTopByNameOrderByVersionDesc(request.name())
            .map(existing -> existing.getVersion() + 1)
            .orElse(1);

        WorkflowDefinition definition = new WorkflowDefinition();
        definition.setName(request.name());
        definition.setDescription(request.description());
        definition.setVersion(nextVersion);
        definition.setActive(true);

        int stepOrder = 0;
        for (WorkflowStepRequest stepRequest : request.steps()) {
            definition.addStep(toStepDefinition(stepRequest, stepOrder));
            stepOrder = stepOrder + 1;
        }

        WorkflowDefinition persisted = workflowDefinitionRepository.save(definition);
        return toResponse(persisted);
    }

    @Override
    @Transactional(readOnly = true)
    public WorkflowDefinitionResponse getDefinition(UUID workflowDefinitionId) {
        WorkflowDefinition definition = workflowDefinitionRepository.findByIdAndActiveTrue(workflowDefinitionId)
            .orElseThrow(() -> new WorkflowDefinitionNotFoundException(workflowDefinitionId));
        return toResponse(definition);
    }

    @Override
    @Transactional(readOnly = true)
    public List<WorkflowDefinitionResponse> listActiveDefinitions() {
        return workflowDefinitionRepository.findAll()
            .stream()
            .filter(WorkflowDefinition::isActive)
            .map(this::toResponse)
            .toList();
    }

    private WorkflowStepDefinition toStepDefinition(WorkflowStepRequest stepRequest, int stepOrder) {
        WorkflowStepDefinition stepDefinition = new WorkflowStepDefinition();
        stepDefinition.setStepOrder(stepOrder);
        stepDefinition.setTaskName(stepRequest.taskName());
        stepDefinition.setCompensationTaskName(stepRequest.compensationTaskName());
        stepDefinition.setTargetTopic(stepRequest.targetTopic());
        stepDefinition.setRetryLimit(stepRequest.retryLimit() != null ? stepRequest.retryLimit() : 3);
        stepDefinition.setTimeoutMs(stepRequest.timeoutMs() != null ? stepRequest.timeoutMs() : 30000L);
        return stepDefinition;
    }

    private WorkflowDefinitionResponse toResponse(WorkflowDefinition definition) {
        List<WorkflowStepSummary> stepSummaries = definition.getSteps()
            .stream()
            .map(step -> new WorkflowStepSummary(
                step.getStepOrder(),
                step.getTaskName(),
                step.getCompensationTaskName(),
                step.getTargetTopic(),
                step.getRetryLimit()
            ))
            .toList();

        return new WorkflowDefinitionResponse(
            definition.getId(),
            definition.getName(),
            definition.getVersion(),
            definition.getDescription(),
            definition.isActive(),
            stepSummaries,
            definition.getCreatedAt()
        );
    }
}