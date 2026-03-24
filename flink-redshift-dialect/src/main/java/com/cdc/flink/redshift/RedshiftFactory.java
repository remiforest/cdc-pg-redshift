package com.cdc.flink.redshift;

import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

public class RedshiftFactory implements JdbcFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:redshift:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new RedshiftDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        throw new UnsupportedOperationException("Redshift catalog is not supported.");
    }
}
