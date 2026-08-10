CREATE TABLE workflow_definition (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_workflow_definition_name_version UNIQUE (name, version)
);

CREATE TABLE workflow_step_definition (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definition (id) ON DELETE CASCADE,
    step_order INTEGER NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    compensation_task_name VARCHAR(255),
    retry_limit INTEGER NOT NULL DEFAULT 3,
    timeout_ms BIGINT NOT NULL DEFAULT 30000,
    target_topic VARCHAR(255) NOT NULL,
    CONSTRAINT uq_step_definition_order UNIQUE (workflow_definition_id, step_order)
);

CREATE INDEX idx_step_definition_workflow_id
    ON workflow_step_definition (workflow_definition_id);