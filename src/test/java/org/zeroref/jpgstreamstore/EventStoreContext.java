package org.zeroref.jpgstreamstore;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EventStoreContext implements AutoCloseable {

    private final DataSource dataSource;
    private final PgEventStore eventStore;

    public EventStoreContext() {
        dataSource = wireDataSource();
        eventStore = new PgEventStore(dataSource);

        initSchema();
    }

    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public PgEventStore getEventStore() {
        return eventStore;
    }

    private void initSchema() {
        try {
            eventStore.createSchema();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close()  {

    }

    private DataSource wireDataSource()
    {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL("jdbc:postgresql://localhost:5432/sqlstreamstore");

        return source;
    }

    public Integer countRecords() {
        int count = 0;

        Statement stmt3 = null;
        try {
            stmt3 = getConnection().createStatement();
            ResultSet rs3 = stmt3.executeQuery("SELECT COUNT(*) AS count FROM tbl_es_event_store");
            while(rs3.next()){
                count = rs3.getInt("count");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }
}
