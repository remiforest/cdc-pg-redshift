package com.cdc.flink.redshift;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RedshiftDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    private RowType rowType;

    @Override
    public String dialectName() {
        return "Redshift";
    }

    @Override
    public void validate(RowType rowType) throws ValidationException {
        this.rowType = rowType;
        super.validate(rowType);
    }

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        this.rowType = rowType;
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

    private String toRedshiftType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return "DECIMAL";
            case CHAR:
            case VARCHAR:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMPTZ";
            default:
                return "VARCHAR";
        }
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // Return empty to force Flink's fallback: SELECT exists + UPDATE/INSERT
        // These statements target the table directly, so Redshift knows the column types.
        // MERGE INTO ... USING (SELECT ...) doesn't work because Redshift can't infer
        // types in a bare SELECT subquery (varchar->timestamp conversion fails).
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
                LogicalTypeRoot.VARBINARY,
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
