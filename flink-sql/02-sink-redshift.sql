-- Flink JDBC sink table targeting Redshift (Ververica)
-- Uses the custom RedshiftDialect (flink-redshift-dialect JAR)
-- which handles upsert via SELECT+UPDATE/INSERT fallback

CREATE TABLE redshift_orders (
  id INT,
  customer_name STRING,
  amount DECIMAL(10,2),
  status STRING,
  created_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:redshift://<REDSHIFT_ENDPOINT>:5439/<REDSHIFT_DB>',
  'table-name' = 'orders',
  'username' = '<REDSHIFT_USER>',
  'password' = '<REDSHIFT_PASSWORD>'
);
