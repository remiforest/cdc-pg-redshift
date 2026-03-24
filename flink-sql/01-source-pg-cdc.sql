-- Flink CDC source table (Ververica)
-- Reads change events from PostgreSQL via Debezium

CREATE TABLE pg_orders (
  id INT,
  customer_name STRING,
  amount DECIMAL(10,2),
  status STRING,
  created_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = '<PG_HOST>',
  'port' = '<PG_PORT>',
  'username' = '<PG_USER>',
  'password' = '<PG_PASSWORD>',
  'database-name' = '<PG_DATABASE>',
  'schema-name' = 'public',
  'table-name' = 'orders',
  'slot.name' = 'flink_slot',
  'decoding.plugin.name' = 'pgoutput',
  'scan.startup.mode' = 'initial'
);
