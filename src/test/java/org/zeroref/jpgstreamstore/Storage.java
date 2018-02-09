package org.zeroref.jpgstreamstore;

import org.zeroref.jpgstreamstore.store.EventStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Storage {

    DataSource dataSource = null;

    public Storage(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int countRecords() {
        int count = 0;

        Statement stmt3 = null;
        try {
            Connection connection = dataSource.getConnection();
            stmt3 = connection.createStatement();
            ResultSet rs3 = stmt3.executeQuery("SELECT COUNT(*) AS count FROM jpg_stream_store_log");
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

        Statement stmt3 = null;
        try {
            Connection connection = dataSource.getConnection();
            stmt3 = connection.createStatement();
            ResultSet rs3 = stmt3.executeQuery("SELECT event_body FROM jpg_stream_store_log");
            while (rs3.next()) {
                content.add(rs3.getString("event_body"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return content;
    }

    public void state(String state) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            connection.createStatement().execute(state);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}
