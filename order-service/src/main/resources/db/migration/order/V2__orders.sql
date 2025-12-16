CREATE TABLE orders (
    order_id      VARCHAR(64)   PRIMARY KEY,
    user_id       VARCHAR(64)   NOT NULL,
    product_id    VARCHAR(64)   NOT NULL,
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    total_amount  NUMERIC(19,2) NOT NULL CHECK (total_amount >= 0),
    status        VARCHAR(32)   NOT NULL,
    cancel_reason TEXT,
    version       BIGINT        NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL
);

-- Operational query: find orders stuck partway through the saga.
CREATE INDEX idx_orders_status_updated_at ON orders (status, updated_at);
