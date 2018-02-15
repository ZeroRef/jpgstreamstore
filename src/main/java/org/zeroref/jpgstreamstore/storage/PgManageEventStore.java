package org.zeroref.jpgstreamstore.storage;

import org.zeroref.jpgstreamstore.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PgManageEventStore implements ManageEventStore {
    private PgSchemaTenant tenant;
    private PgConnectionFactory connections;

    PgManageEventStore(PgConnectionFactory connections) {
        this.connections = connections;
        this.tenant = connections.getTenant();
    }

    @Override
    public List<StreamId> listStreams() {
        String sql = "select stream_name, max(stream_version) from jpg_stream_store_log " +
                "group by stream_name ";

        try (Connection conn = connections.open();
             PreparedStatement sttmt =conn.prepareStatement(tenant.prepare(sql))) {

            try (ResultSet result = sttmt.executeQuery()) {
                return this.buildEventStreamIds(result);
            }

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query event for list streams because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public List<StoreRecord> fetchDelta(long position) {
        String sql = "SELECT event_id, event_body, stream_name, stream_version FROM jpg_stream_store_log "
                + "WHERE event_id > ? ORDER BY event_id";

        try (Connection conn = connections.open();
             PreparedStatement sttmt = conn.prepareStatement(tenant.prepare(sql) )) {

            sttmt.setLong(1, position);

            try (ResultSet result = sttmt.executeQuery()) {
                return this.buildEventSequence(result);
            }
        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query event for sequence since: "
                            + position
                            + " because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public void purge() {
        String sql = "delete from jpg_stream_store_log";
        try (Connection conn = connections.open();
             Statement sttmt = conn.createStatement()) {

            sttmt.execute(tenant.prepare(sql));

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public void createSchema() throws IOException {
        String logSql = readResource("create_log.sql");

        try (Connection conn = connections.open();
             Statement sttmt = conn.createStatement()) {

            if(tenant.applicable()){
                String schemaSql = "create schema if not exists " + tenant.getSchemaName();
                sttmt.execute(tenant.prepare(schemaSql));
            }

            sttmt.execute(tenant.prepare(logSql));

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        }
    }

    public static String readResource(String filename)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        try(InputStream is = PgManageEventStore.class.getClassLoader().getResourceAsStream(filename);
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr)){
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }

        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<StreamId> buildEventStreamIds(ResultSet result) throws SQLException {
        List<StreamId> events = new ArrayList<>();

        while (result.next()) {
            String streamName = result.getString("stream_name");
            events.add(new StreamId(streamName));
        }

        return events;
    }

    @SuppressWarnings("unchecked")
    private List<StoreRecord> buildEventSequence(ResultSet result) throws Exception {

        List<StoreRecord> events = new ArrayList<>();

        while (result.next()) {
            long eventId = result.getLong("event_id");
            String eventBody = result.getString("event_body");
            String streamName = result.getString("stream_name");
            int streamVersion = result.getInt("stream_version");

            events.add(new StoreRecord(eventId, eventBody, streamName, streamVersion));
        }

        return events;
    }
}
