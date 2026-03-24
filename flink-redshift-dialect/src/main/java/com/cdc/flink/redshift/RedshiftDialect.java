package com.cdc.flink.redshift;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

public class RedshiftDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "Redshift";
    }

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new RedshiftDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.amazon.redshift.jdbc42.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // Return empty to force Flink's fallback: SELECT exists + UPDATE/INSERT.
        // Redshift doesn't support ON CONFLICT (PostgreSQL 9.5+), and MERGE INTO
        // with USING (SELECT ...) fails on type inference for parameterized queries.
        // The fallback targets the table directly, so Redshift resolves column types.
        //
        // Trade-off: 3 round-trips per row instead of 1 MERGE.
        // Acceptable for low-to-moderate throughput CDC workloads.
        return Optional.empty();
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 38));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 6));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }
}
