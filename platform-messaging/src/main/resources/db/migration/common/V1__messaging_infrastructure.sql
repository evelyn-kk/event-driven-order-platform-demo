-- Delivery-guarantee tables shared by every service that talks to Kafka.
-- Services layer their own business tables on top starting at V2.

CREATE TABLE outbox_message (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_id       VARCHAR(64)  NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    message_key    VARCHAR(128) NOT NULL,
    payload_type   VARCHAR(255) NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INTEGER      NOT NULL DEFAULT 0,
    last_error     TEXT,
    CONSTRAINT uq_outbox_event_id UNIQUE (event_id)
);

-- The relay only ever reads unpublished rows, and published rows quickly outnumber them by orders
-- of magnitude. A partial index keeps the relay's scan proportional to the backlog rather than to
-- the table, so drain latency stays flat as history accumulates.
CREATE INDEX idx_outbox_unpublished
    ON outbox_message (created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_event (
    event_id       VARCHAR(64)  NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (event_id, consumer_group)
);

-- Supports pruning markers older than the retention of the topics they guard.
CREATE INDEX idx_processed_event_processed_at ON processed_event (processed_at);
