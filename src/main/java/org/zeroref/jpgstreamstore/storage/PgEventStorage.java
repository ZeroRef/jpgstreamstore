package org.zeroref.jpgstreamstore.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.zeroref.jpgstreamstore.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PgEventStorage implements EventStore {

    private PGConnectionPoolDataSource dataSource;
    private Gson serializer = new GsonBuilder().create();

    public PgEventStorage(String connectionString) {
        PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
        ds.setUrl(connectionString);

        this.dataSource = ds;
    }

    public static String readResource(String filename)
            throws IOException {

        StringBuffer sb = new StringBuffer();

        try(InputStream is = PgEventStorage.class.getClassLoader().getResourceAsStream(filename);
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr)){
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }

        return sb.toString();
    }

    @Override
    public AppendResult appendToStream(StreamId streamId, List<EventData> anEvents) {
        return appendToStream(streamId, ExpectedVersion.Any, anEvents);
    }

    @Override
    public AppendResult appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents) {

        Connection conn = this.connection();

        try {
            conn.setAutoCommit(false);

            if (expectedVersion == ExpectedVersion.NoStream) {
                assertStreamDoesNotExist(streamId, conn);
            }

            int index = ExpectedVersion.NoStream == expectedVersion ? 1 : 0;

            for (EventData event : anEvents) {
                this.appendEventStore(conn, streamId, index++, event, expectedVersion);
            }

            conn.commit();

            return new AppendResult(index + expectedVersion);

        } catch (Throwable t1) {
            try {
                conn.rollback();
            } catch (Throwable t2) {
                // ignore
            }

            throw new EventStoreAppendException(
                    "Could not append to event store because: "
                            + t1.getMessage(),
                    t1);
        }finally {
            try {
                conn.close();
            } catch (Throwable t2) {
                // ignore
            }
        }
    }

    private void assertStreamDoesNotExist(StreamId streamId, Connection connection) throws SQLException {
        String sql = "SELECT count(*) FROM jpg_stream_store_log WHERE stream_name = ? ";

        try (PreparedStatement sttmt = connection.prepareStatement(sql)) {
            sttmt.setString(1, streamId.streamName());

            try (ResultSet rs3 = sttmt.executeQuery()) {
                if (rs3.next()) {
                    int count = rs3.getInt("count");

                    if (count > 0) {
                        throw new EventStoreAppendException(
                                "Could not append to event store because stream exists : " + streamId);
                    }
                }
            }
        }
    }

    public List<StreamId> listStreams() {
        String sql = "select stream_name, max(stream_version) from jpg_stream_store_log group by stream_name ";

        try (Connection conn = this.connection();
             PreparedStatement sttmt =conn.prepareStatement(sql)) {

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
    public List<StoreRecord> eventsSince(long position) {
        String sql = "SELECT event_id, event_body, stream_name, stream_version FROM jpg_stream_store_log "
                + "WHERE event_id > ? ORDER BY event_id";

        try (Connection conn = this.connection();
             PreparedStatement sttmt = conn.prepareStatement(sql)) {

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
    public EventStream eventStreamSince(StreamId anIdentity, int version) {

        String sql = "SELECT stream_version, event_body FROM jpg_stream_store_log "
                + "WHERE stream_name = ? AND stream_version >= ? ORDER BY stream_version";

        try (Connection conn = this.connection();
             PreparedStatement sttmt = conn.prepareStatement(sql)) {

            sttmt.setString(1, anIdentity.streamName());
            sttmt.setInt(2, version);

            try (ResultSet result = sttmt.executeQuery()) {
                EventStream eventStream = this.buildEventStream(result);

                if (eventStream.version() == 0) {
                    throw new EventStoreException(
                            "There is no such event stream: "
                                    + anIdentity.streamName()
                                    + " : "
                                    + version);
                }

                return eventStream;
            }
        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query event stream for: "
                            + anIdentity.streamName()
                            + " since version: "
                            + version
                            + " because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public EventStream fullEventStreamFor(StreamId anIdentity) {
        String sql = "SELECT stream_version, event_body FROM jpg_stream_store_log "
                + "WHERE stream_name = ? ORDER BY stream_version";

        try (Connection conn = this.connection();
             PreparedStatement sttmt = conn.prepareStatement(sql)) {

            sttmt.setString(1, anIdentity.streamName());

            try (ResultSet result = sttmt.executeQuery()) {
                return this.buildEventStream(result);
            }
        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query full event stream for: "
                            + anIdentity.streamName()
                            + " because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public void deleteStream(StreamId anIdentity) {
        String sql = "delete from jpg_stream_store_log WHERE stream_name = ? ";

        try (Connection conn = this.connection();
             PreparedStatement sttmt = conn.prepareStatement(sql)) {

            sttmt.setString(1, anIdentity.streamName());
            sttmt.execute();

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    public void purge() {
        String sql = "delete from jpg_stream_store_log";
        try (Connection conn = this.connection();
             Statement sttmt = conn.createStatement()) {

            sttmt.execute(sql);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    public void createSchema() throws IOException {
        String content = readResource("create_log.sql");

        try (Connection conn = this.connection();
             Statement sttmt = conn.createStatement()) {
            sttmt.execute(content);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        }
    }

    private void appendEventStore(
            Connection conn,
            StreamId anIdentity,
            int anIndex,
            EventData aEventData, int expectedVersion)
            throws Exception {

        PreparedStatement ps;

        if (expectedVersion == ExpectedVersion.Any) {
            ps = conn
                    .prepareStatement(
                            "INSERT INTO jpg_stream_store_log " +
                                    "(event_body, stream_name, stream_version)" +
                                    "VALUES(?, ?, (select coalesce(max(stream_version+1),1) from jpg_stream_store_log where stream_name = ?) )");

            ps.setString(1, serializer.toJson(aEventData.getProps()));
            ps.setString(2, anIdentity.streamName());
            ps.setString(3, anIdentity.streamName());
        } else {
            ps = conn
                    .prepareStatement(
                            "INSERT INTO jpg_stream_store_log " +
                                    "(event_body, stream_name, stream_version)" +
                                    "VALUES(?, ?, ?)");

            ps.setString(1, serializer.toJson(aEventData.getProps()));
            ps.setString(2, anIdentity.streamName());
            ps.setInt(3, expectedVersion + anIndex);
        }

        ps.executeUpdate();
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

    @SuppressWarnings("unchecked")
    private EventStream buildEventStream(ResultSet result) throws Exception {

        List<EventData> events = new ArrayList<>();

        int version = 0;

        while (result.next()) {
            version = result.getInt("stream_version");
            String eventBody = result.getString("event_body");

            Type eventClass = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> eventData = serializer.fromJson(eventBody, eventClass);

            events.add(new EventData(eventData));
        }

        return new DefaultEventStream(events, version);
    }

    private Connection connection() {
        Connection conn;

        try {
            conn = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot acquire database conn.");
        }

        return conn;
    }
}
