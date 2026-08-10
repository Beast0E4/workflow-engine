package com.enginecorp.workfloworchestrator.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "workflow_step_definition")
@Getter
@Setter
@NoArgsConstructor
public class WorkflowStepDefinition {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_definition_id", nullable = false)
    private WorkflowDefinition workflowDefinition;

    @Column(name = "step_order", nullable = false)
    private Integer stepOrder;

    @Column(name = "task_name", nullable = false)
    private String taskName;

    @Column(name = "compensation_task_name")
    private String compensationTaskName;

    @Column(name = "retry_limit", nullable = false)
    private Integer retryLimit = 3;

    @Column(name = "timeout_ms", nullable = false)
    private Long timeoutMs = 30000L;

    @Column(name = "target_topic", nullable = false)
    private String targetTopic;
}