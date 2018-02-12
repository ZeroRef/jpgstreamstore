package org.zeroref.jpgstreamstore;

import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.storage.PgSchemaTenant;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Storage {

    PGSimpleDataSource dataSource;
    PgSchemaTenant tenant;

    public Storage(PGSimpleDataSource dataSource) {
        this.dataSource = dataSource;
        this.tenant = new PgSchemaTenant(dataSource.getCurrentSchema());

        dropLogIfExists();
    }

    private void dropLogIfExists() {
        String sql = "DROP TABLE IF EXISTS jpg_stream_store_log";
        state(tenant.prepare(sql));
    }

    public int countRecords() {
        int count = 0;

        String sql = "SELECT COUNT(*) AS count FROM jpg_stream_store_log";

        String prepare = tenant.prepare(sql);

        try (Connection connection = dataSource.getConnection();
             Statement stmt3 = connection.createStatement();
             ResultSet rs3 = stmt3.executeQuery(prepare)) {

            while (rs3.next()) {
                count = rs3.getInt("count");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public List<String> listPayloads() {
        List<String> content = new ArrayList<>();

        String sql = "SELECT event_body FROM jpg_stream_store_log";

        String prepare = tenant.prepare(sql);

        try (Connection connection = dataSource.getConnection();
             Statement stmt3 = connection.createStatement();
             ResultSet rs3 = stmt3.executeQuery(prepare)) {

            while (rs3.next()) {
                content.add(rs3.getString("event_body"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return content;
    }

    public void state(String state) {

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(state);
        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        }
    }
}
