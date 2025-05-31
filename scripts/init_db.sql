-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    product_category VARCHAR(100),
    transaction_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions(transaction_time);
CREATE INDEX IF NOT EXISTS idx_transactions_country ON transactions(country);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON transactions(currency);
CREATE INDEX IF NOT EXISTS idx_transactions_batch ON transactions(batch_id);

-- Create FX rates tracking table
CREATE TABLE IF NOT EXISTS fx_rates_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    base_currency VARCHAR(3) DEFAULT 'USD',
    target_currency VARCHAR(3) NOT NULL,
    rate DECIMAL(10,6) NOT NULL,
    source VARCHAR(50) DEFAULT 'kafka_stream'
);

CREATE INDEX IF NOT EXISTS idx_fx_rates_timestamp ON fx_rates_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_fx_rates_currency ON fx_rates_log(target_currency);

-- Insert some sample FX rates for initial testing
INSERT INTO fx_rates_log (timestamp, target_currency, rate) VALUES
(NOW(), 'EUR', 0.85),
(NOW(), 'GBP', 0.73),
(NOW(), 'CAD', 1.35),
(NOW(), 'JPY', 110.0),
(NOW(), 'AUD', 1.45);