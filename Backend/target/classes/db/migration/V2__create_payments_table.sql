CREATE TABLE payments (
    id BIGSERIAL PRIMARY KEY,
    payment_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    amount NUMERIC(19, 4) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_payments_payment_id UNIQUE (payment_id)
);

CREATE INDEX idx_payments_user_id ON payments (user_id);
