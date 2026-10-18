CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(128) NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    total_amount NUMERIC(19, 4) NOT NULL,
    item_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_orders_order_id UNIQUE (order_id)
);

CREATE INDEX idx_orders_customer_id ON orders (customer_id);
