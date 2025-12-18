CREATE TABLE inventory_item (
    product_id VARCHAR(64) PRIMARY KEY,
    available  INTEGER     NOT NULL CHECK (available >= 0),
    reserved   INTEGER     NOT NULL CHECK (reserved  >= 0)
);

CREATE TABLE stock_reservation (
    order_id   VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL REFERENCES inventory_item (product_id),
    quantity   INTEGER     NOT NULL CHECK (quantity > 0),
    state      VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ
);

-- Operational query: reservations still holding stock, oldest first. A hold that never settles is
-- a compensation that never ran, and it is invisible unless you can list them.
CREATE INDEX idx_reservation_open ON stock_reservation (created_at) WHERE state = 'RESERVED';

-- Seed catalogue. Products absent from this table have no stock, so an order for an unknown SKU
-- takes the insufficient-inventory branch rather than silently inventing supply.
INSERT INTO inventory_item (product_id, available, reserved) VALUES
    ('SKU-1001', 100, 0),
    ('SKU-1002', 100, 0),
    ('SKU-1003', 100, 0),
    ('SKU-1004',  10, 0),
    ('SKU-1005',   0, 0);
