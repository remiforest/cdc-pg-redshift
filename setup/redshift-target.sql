-- Redshift target table setup
-- Run this on Redshift Serverless

CREATE TABLE IF NOT EXISTS orders (
  id INT PRIMARY KEY,
  customer_name VARCHAR(255),
  amount DECIMAL(10,2),
  status VARCHAR(50),
  created_at TIMESTAMPTZ  -- TIMESTAMPTZ required for JDBC driver compatibility
);

-- Grant permissions to the Flink user
GRANT USAGE ON SCHEMA public TO demo_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.orders TO demo_user;
