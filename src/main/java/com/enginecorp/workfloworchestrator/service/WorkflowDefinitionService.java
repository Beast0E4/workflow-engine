package com.enginecorp.workfloworchestrator.service;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowDefinitionRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowDefinitionResponse;
import java.util.List;
import java.util.UUID;

public interface WorkflowDefinitionService {

    WorkflowDefinitionResponse createDefinition(WorkflowDefinitionRequest request);

    WorkflowDefinitionResponse getDefinition(UUID workflowDefinitionId);

    List<WorkflowDefinitionResponse> listActiveDefinitions();
}