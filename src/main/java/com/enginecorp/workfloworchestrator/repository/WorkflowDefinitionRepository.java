package com.enginecorp.workfloworchestrator.repository;

import com.enginecorp.workfloworchestrator.model.WorkflowDefinition;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinition, UUID> {

    Optional<WorkflowDefinition> findByIdAndActiveTrue(UUID id);

    Optional<WorkflowDefinition> findTopByNameOrderByVersionDesc(String name);
}