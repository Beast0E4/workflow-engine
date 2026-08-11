package com.enginecorp.workfloworchestrator.service;

import com.enginecorp.workfloworchestrator.model.StepExecution;
import com.enginecorp.workfloworchestrator.model.WorkflowInstance;

public interface StepDispatchService {

    void dispatchStep(WorkflowInstance workflowInstance, StepExecution stepExecution, int attemptNumber);
}