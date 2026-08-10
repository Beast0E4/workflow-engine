package com.enginecorp.workfloworchestrator.repository;

import com.enginecorp.workfloworchestrator.model.WorkflowInstance;
import com.enginecorp.workfloworchestrator.model.WorkflowStatus;
import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, UUID> {

    @Lock(LockModeType.OPTIMISTIC_FORCE_INCREMENT)
    @Query("select workflowInstance from WorkflowInstance workflowInstance where workflowInstance.id = :id")
    Optional<WorkflowInstance> findByIdForUpdate(UUID id);

    List<WorkflowInstance> findByStatus(WorkflowStatus status);
}