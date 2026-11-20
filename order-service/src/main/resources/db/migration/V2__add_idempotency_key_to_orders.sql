ALTER TABLE orders ADD COLUMN idempotency_key VARCHAR(100);

UPDATE orders SET idempotency_key = id::text WHERE idempotency_key IS NULL;

ALTER TABLE orders ALTER COLUMN idempotency_key SET NOT NULL;
ALTER TABLE orders ADD CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key);
