CREATE TABLE payment (
    order_id       VARCHAR(64)   PRIMARY KEY,
    user_id        VARCHAR(64)   NOT NULL,
    amount         NUMERIC(19,2) NOT NULL CHECK (amount >= 0),
    state          VARCHAR(16)   NOT NULL,
    transaction_id VARCHAR(64),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ   NOT NULL,
    settled_at     TIMESTAMPTZ
);

-- Operational query: charges that were registered but never settled.
CREATE INDEX idx_payment_pending ON payment (created_at) WHERE state = 'PENDING';
