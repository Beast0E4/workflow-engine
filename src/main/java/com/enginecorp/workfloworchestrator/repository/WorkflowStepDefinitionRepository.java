package com.enginecorp.workfloworchestrator.repository;

import com.enginecorp.workfloworchestrator.model.WorkflowStepDefinition;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowStepDefinitionRepository extends JpaRepository<WorkflowStepDefinition, UUID> {
}