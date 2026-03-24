-- PostgreSQL source table setup
-- Run this on the source PostgreSQL (e.g. Aiven)

CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  customer_name VARCHAR(255),
  amount DECIMAL(10,2),
  status VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Required for CDC UPDATE/DELETE support (Debezium needs the "before" image)
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Test data
INSERT INTO orders (customer_name, amount, status) VALUES ('Alice', 100.00, 'NEW');
INSERT INTO orders (customer_name, amount, status) VALUES ('Bob', 200.00, 'NEW');
INSERT INTO orders (customer_name, amount, status) VALUES ('Charlie', 150.00, 'PENDING');
