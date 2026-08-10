package com.enginecorp.workfloworchestrator.service;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowStartRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowInstanceResponse;
import java.util.UUID;

public interface WorkflowOrchestrationService {

    WorkflowInstanceResponse startWorkflow(WorkflowStartRequest request);

    WorkflowInstanceResponse getInstance(UUID workflowInstanceId);

    void advanceAfterStepSuccess(UUID workflowInstanceId, UUID stepExecutionId);

    void handleStepFailure(UUID workflowInstanceId, UUID stepExecutionId, String errorMessage);

    void retryEligibleStep(UUID workflowInstanceId, UUID stepExecutionId);
}