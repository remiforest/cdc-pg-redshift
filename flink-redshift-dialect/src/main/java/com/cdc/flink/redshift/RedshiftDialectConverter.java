package com.cdc.flink.redshift;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.types.logical.RowType;

public class RedshiftDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public RedshiftDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Redshift";
    }
}
