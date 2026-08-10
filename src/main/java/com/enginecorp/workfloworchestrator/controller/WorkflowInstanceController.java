package com.enginecorp.workfloworchestrator.controller;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowStartRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowInstanceResponse;
import com.enginecorp.workfloworchestrator.service.WorkflowOrchestrationService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workflow-instances")
public class WorkflowInstanceController {

    private final WorkflowOrchestrationService workflowOrchestrationService;

    public WorkflowInstanceController(WorkflowOrchestrationService workflowOrchestrationService) {
        this.workflowOrchestrationService = workflowOrchestrationService;
    }

    @PostMapping
    public ResponseEntity<WorkflowInstanceResponse> startWorkflow(@Valid @RequestBody WorkflowStartRequest request) {
        WorkflowInstanceResponse response = workflowOrchestrationService.startWorkflow(request);
        return ResponseEntity.created(URI.create("/api/v1/workflow-instances/" + response.id())).body(response);
    }

    @GetMapping("/{workflowInstanceId}")
    public ResponseEntity<WorkflowInstanceResponse> getInstance(@PathVariable UUID workflowInstanceId) {
        return ResponseEntity.ok(workflowOrchestrationService.getInstance(workflowInstanceId));
    }
}