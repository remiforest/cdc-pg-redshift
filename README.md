# CDC Pipeline: PostgreSQL → Flink → Redshift

Real-time Change Data Capture pipeline using Flink CDC on Ververica Cloud to replicate data from PostgreSQL (Aiven) to Amazon Redshift Serverless.

## Architecture

```
PostgreSQL (Aiven)  →  Flink CDC (Ververica)  →  Redshift Serverless
   source                  processing                  sink
   (Debezium)             (streaming)              (JDBC + custom dialect)
```

## The Problem

Flink's JDBC connector uses PostgreSQL's `ON CONFLICT ... DO UPDATE` for upserts, but Redshift doesn't support this syntax (Redshift is based on PostgreSQL 8.x). This project includes a **custom Flink JDBC dialect for Redshift** that handles this incompatibility.

## Custom Redshift Dialect

The `flink-redshift-dialect` module is a Flink JDBC dialect plugin that:
- Registers a `JdbcFactory` for `jdbc:redshift://` URLs via Java SPI
- Returns `Optional.empty()` from `getUpsertStatement()` to force Flink's fallback upsert pattern (SELECT exists + UPDATE/INSERT) instead of generating unsupported `ON CONFLICT` or `MERGE` syntax
- Provides proper type mappings for Redshift data types

### Build

```bash
cd flink-redshift-dialect
mvn clean package
```

### Required JARs for Ververica

Upload these 3 JARs as artifacts in your Ververica deployment:

1. `flink-redshift-dialect/target/flink-redshift-dialect-1.0.0.jar` (custom dialect)
2. `flink-connector-jdbc-3.3.0-1.20.jar` ([Maven Central](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc/3.3.0-1.20))
3. `redshift-jdbc42-2.2.5.jar` ([Maven Central](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42/2.2.5))

## Setup

### 1. PostgreSQL source

Run `setup/postgres-source.sql` on your PostgreSQL instance.

Key requirement: `ALTER TABLE orders REPLICA IDENTITY FULL` is needed for Debezium to capture UPDATE/DELETE events.

### 2. Redshift target

Run `setup/redshift-target.sql` on your Redshift cluster.

Note: use `TIMESTAMPTZ` (not `TIMESTAMP`) for timestamp columns to avoid type conversion issues with the JDBC driver.

### 3. Ververica

1. Upload the 3 JARs to your deployment
2. Execute the SQL files in `flink-sql/` in order (01, 02, 03)
3. Replace placeholders (`<PG_HOST>`, `<REDSHIFT_ENDPOINT>`, etc.) with your values

## Test CDC

```sql
-- In PostgreSQL: insert
INSERT INTO orders (customer_name, amount, status) VALUES ('Test', 99.99, 'NEW');

-- In PostgreSQL: update
UPDATE orders SET status = 'SHIPPED' WHERE customer_name = 'Test';

-- In Redshift: verify
SELECT * FROM orders;
```

## Lessons Learned

- Redshift doesn't support `ON CONFLICT` (PostgreSQL 9.5+ syntax) or reliable `MERGE` with bare `SELECT` subqueries
- Redshift's `MERGE INTO ... USING (SELECT ...)` fails on type inference for parameters (varchar→timestamp)
- The fallback upsert pattern (SELECT + UPDATE/INSERT) works because queries target the table directly, giving Redshift column type context
- `REPLICA IDENTITY FULL` is required on the PG source for UPDATE/DELETE CDC events
- Use `TIMESTAMPTZ` in Redshift for timestamp columns receiving data via JDBC
