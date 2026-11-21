CREATE TABLE inventory (
    product_id UUID PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    available_quantity INT NOT NULL CHECK (available_quantity >= 0),
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Seed initial product for local testing
INSERT INTO inventory (product_id, sku, available_quantity, version)
VALUES ('770e8400-e29b-41d4-a716-446655441111', 'IPHONE-16-PRO-256', 10, 0);
