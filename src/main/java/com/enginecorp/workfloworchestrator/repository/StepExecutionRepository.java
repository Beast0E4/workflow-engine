package com.enginecorp.workfloworchestrator.repository;

import com.enginecorp.workfloworchestrator.model.StepExecution;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StepExecutionRepository extends JpaRepository<StepExecution, UUID>, StepExecutionRepositoryCustom {

    List<StepExecution> findByWorkflowInstanceId(UUID workflowInstanceId);
}