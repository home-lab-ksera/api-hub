CREATE TABLE notifications (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    producer text NOT NULL,
    event_id text NOT NULL,
    channel text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'sending', 'retry', 'sent', 'uncertain', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    claim_token uuid,
    slack_ts text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (producer, event_id, channel)
);
CREATE INDEX notifications_due ON notifications (available_at, id)
    WHERE state IN ('pending', 'retry');
CREATE INDEX notifications_leases ON notifications (lease_until)
    WHERE state = 'sending';
