package org.zeroref.jpgstreamstore.storage;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class PgConnectionFactory {
    private PgSchemaTenant tenant;
    private PGConnectionPoolDataSource dataSource;

    public PgConnectionFactory(String connectionString) {
        PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
        ds.setUrl(connectionString);

        this.tenant = new PgSchemaTenant(ds.getCurrentSchema());
        this.dataSource = ds;
    }

    public Connection open() {
        Connection conn;

        try {
            conn = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot acquire database conn.");
        }

        return conn;
    }

    public PgSchemaTenant getTenant() {
        return tenant;
    }
}
