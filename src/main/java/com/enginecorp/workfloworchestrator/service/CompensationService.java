package com.enginecorp.workfloworchestrator.service;

import com.enginecorp.workfloworchestrator.dto.messaging.CompensationResultMessage;
import java.util.UUID;

public interface CompensationService {

    void beginCompensation(UUID workflowInstanceId);

    void handleCompensationResult(CompensationResultMessage message);
}