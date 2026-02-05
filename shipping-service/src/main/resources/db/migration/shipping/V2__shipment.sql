CREATE TABLE shipment (
    order_id        VARCHAR(64) PRIMARY KEY,
    shipment_id     VARCHAR(64) NOT NULL UNIQUE,
    carrier         VARCHAR(64) NOT NULL,
    tracking_number VARCHAR(64) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);
