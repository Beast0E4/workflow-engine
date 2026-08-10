package com.enginecorp.workfloworchestrator.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorkflowOrchestrationServiceImplTest {

    @Mock
    private WorkflowDefinitionRepository workflowDefinitionRepository;

    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Mock
    private StepDispatchService stepDispatchService;

    @Mock
    private CompensationService compensationService;

    private DistributedLockService distributedLockService;

    private WorkflowOrchestrationServiceImpl orchestrationService;

    private WorkflowInstance instance;
    private StepExecution firstStep;
    private StepExecution secondStep;

    @BeforeEach
    void setUp() {
        distributedLockService = new PassThroughLockService();
        orchestrationService = new WorkflowOrchestrationServiceImpl(
            workflowDefinitionRepository,
            workflowInstanceRepository,
            stepDispatchService,
            compensationService,
            distributedLockService,
            new ObjectMapper()
        );

        WorkflowDefinition definition = new WorkflowDefinition();
        definition.setName("order-fulfillment");
        definition.setVersion(1);

        WorkflowStepDefinition firstStepDefinition = new WorkflowStepDefinition();
        firstStepDefinition.setStepOrder(0);
        firstStepDefinition.setTaskName("reserve-inventory");
        firstStepDefinition.setCompensationTaskName("release-inventory");
        firstStepDefinition.setTargetTopic("inventory.tasks");
        firstStepDefinition.setRetryLimit(2);

        WorkflowStepDefinition secondStepDefinition = new WorkflowStepDefinition();
        secondStepDefinition.setStepOrder(1);
        secondStepDefinition.setTaskName("charge-payment");
        secondStepDefinition.setCompensationTaskName("refund-payment");
        secondStepDefinition.setTargetTopic("payment.tasks");
        secondStepDefinition.setRetryLimit(2);

        definition.addStep(firstStepDefinition);
        definition.addStep(secondStepDefinition);

        instance = new WorkflowInstance();
        instance.setWorkflowDefinition(definition);
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCurrentStepIndex(0);

        firstStep = new StepExecution();
        firstStep.setStepDefinition(firstStepDefinition);
        firstStep.setStatus(StepStatus.DISPATCHED);
        instance.addStepExecution(firstStep);

        secondStep = new StepExecution();
        secondStep.setStepDefinition(secondStepDefinition);
        secondStep.setStatus(StepStatus.PENDING);
        instance.addStepExecution(secondStep);
    }

    @Test
    void advancingAfterFirstStepSuccessDispatchesTheNextStepWithoutCompletingTheInstance() {
        when(workflowInstanceRepository.findByIdForUpdate(instance.getId())).thenReturn(java.util.Optional.of(instance));
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrationService.advanceAfterStepSuccess(instance.getId(), firstStep.getId());

        assertThat(firstStep.getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(instance.getCurrentStepIndex()).isEqualTo(1);
        assertThat(instance.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
        verify(stepDispatchService).dispatchStep(instance, secondStep, 1);
    }

    @Test
    void advancingAfterFinalStepSuccessMarksTheInstanceCompleted() {
        firstStep.setStatus(StepStatus.COMPLETED);
        instance.setCurrentStepIndex(1);
        secondStep.setStatus(StepStatus.DISPATCHED);

        when(workflowInstanceRepository.findByIdForUpdate(instance.getId())).thenReturn(java.util.Optional.of(instance));
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrationService.advanceAfterStepSuccess(instance.getId(), secondStep.getId());

        assertThat(secondStep.getStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(instance.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void failureWithinRetryBudgetSchedulesNextRetryInsteadOfCompensating() {
        when(workflowInstanceRepository.findByIdForUpdate(instance.getId())).thenReturn(java.util.Optional.of(instance));
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrationService.handleStepFailure(instance.getId(), firstStep.getId(), "inventory service timed out");

        assertThat(firstStep.getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(firstStep.getAttemptCount()).isEqualTo(1);
        assertThat(firstStep.getNextRetryAt()).isNotNull();
        assertThat(instance.getStatus()).isEqualTo(WorkflowStatus.RUNNING);
        verify(compensationService, org.mockito.Mockito.never()).beginCompensation(any(UUID.class));
    }

    @Test
    void failureExceedingRetryBudgetTransitionsInstanceIntoCompensation() {
        firstStep.setAttemptCount(2);
        firstStep.getStepDefinition().setRetryLimit(2);

        when(workflowInstanceRepository.findByIdForUpdate(instance.getId())).thenReturn(java.util.Optional.of(instance));
        when(workflowInstanceRepository.save(any(WorkflowInstance.class))).thenAnswer(invocation -> invocation.getArgument(0));

        orchestrationService.handleStepFailure(instance.getId(), firstStep.getId(), "inventory service unavailable");

        assertThat(firstStep.getStatus()).isEqualTo(StepStatus.FAILED);
        assertThat(firstStep.getNextRetryAt()).isNull();
        assertThat(instance.getStatus()).isEqualTo(WorkflowStatus.COMPENSATING);
        assertThat(instance.getFailureReason()).isEqualTo("inventory service unavailable");

        ArgumentCaptor<UUID> instanceIdCaptor = ArgumentCaptor.forClass(UUID.class);
        verify(compensationService).beginCompensation(instanceIdCaptor.capture());
        assertThat(instanceIdCaptor.getValue()).isEqualTo(instance.getId());
    }

    private static final class PassThroughLockService implements DistributedLockService {

        @Override
        public <T> T executeWithLock(String lockKey, Supplier<T> action) {
            return action.get();
        }

        @Override
        public void executeWithLock(String lockKey, Runnable action) {
            action.run();
        }
    }
}