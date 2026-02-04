-- A deliberately deep SKU for load testing.
--
-- Added as a new migration rather than by editing V2: Flyway records a checksum per applied
-- migration, so changing one that has already run makes every existing database fail validation
-- at startup. Forward-only is the rule even when the change looks trivial.
--
-- Without this, a load run of more than a hundred orders exhausts the seeded catalogue and the
-- remainder measure the cancellation path instead of the fulfillment path.
INSERT INTO inventory_item (product_id, available, reserved)
VALUES ('SKU-LOADTEST', 100000000, 0);
