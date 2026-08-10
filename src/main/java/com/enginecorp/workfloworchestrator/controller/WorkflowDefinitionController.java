package com.enginecorp.workfloworchestrator.controller;

import com.enginecorp.workfloworchestrator.dto.request.WorkflowDefinitionRequest;
import com.enginecorp.workfloworchestrator.dto.response.WorkflowDefinitionResponse;
import com.enginecorp.workfloworchestrator.service.WorkflowDefinitionService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workflow-definitions")
public class WorkflowDefinitionController {

    private final WorkflowDefinitionService workflowDefinitionService;

    public WorkflowDefinitionController(WorkflowDefinitionService workflowDefinitionService) {
        this.workflowDefinitionService = workflowDefinitionService;
    }

    @PostMapping
    public ResponseEntity<WorkflowDefinitionResponse> createDefinition(@Valid @RequestBody WorkflowDefinitionRequest request) {
        WorkflowDefinitionResponse response = workflowDefinitionService.createDefinition(request);
        return ResponseEntity.created(URI.create("/api/v1/workflow-definitions/" + response.id())).body(response);
    }

    @GetMapping("/{workflowDefinitionId}")
    public ResponseEntity<WorkflowDefinitionResponse> getDefinition(@PathVariable UUID workflowDefinitionId) {
        return ResponseEntity.ok(workflowDefinitionService.getDefinition(workflowDefinitionId));
    }

    @GetMapping
    public ResponseEntity<List<WorkflowDefinitionResponse>> listActiveDefinitions() {
        return ResponseEntity.ok(workflowDefinitionService.listActiveDefinitions());
    }
}