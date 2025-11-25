-- Add backoff policy columns for retry delay calculation
-- backoff_kind: 'none' (immediate), 'linear', 'exponential'
-- backoff_base_delay_ms: base delay in milliseconds

ALTER TABLE daemon_action_ledger
    ADD COLUMN backoff_kind TEXT NOT NULL DEFAULT 'none',
    ADD COLUMN backoff_base_delay_ms INTEGER NOT NULL DEFAULT 0;
