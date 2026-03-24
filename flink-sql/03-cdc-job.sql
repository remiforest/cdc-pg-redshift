-- CDC pipeline: replicate all changes from PG to Redshift
INSERT INTO redshift_orders
SELECT * FROM pg_orders;
