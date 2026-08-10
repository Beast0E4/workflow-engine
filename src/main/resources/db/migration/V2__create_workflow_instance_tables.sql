CREATE TABLE workflow_instance (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definition (id),
    status VARCHAR(32) NOT NULL,
    current_step_index INTEGER NOT NULL DEFAULT 0,
    payload JSONB,
    failure_reason TEXT,
    lock_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_workflow_instance_status
    ON workflow_instance (status);

CREATE TABLE step_execution (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_instance_id UUID NOT NULL REFERENCES workflow_instance (id) ON DELETE CASCADE,
    step_definition_id UUID NOT NULL REFERENCES workflow_step_definition (id),
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_step_execution_workflow_instance_id
    ON step_execution (workflow_instance_id);

CREATE INDEX idx_step_execution_retry_lookup
    ON step_execution (status, next_retry_at);