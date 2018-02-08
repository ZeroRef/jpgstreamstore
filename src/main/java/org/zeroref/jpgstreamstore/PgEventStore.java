package org.zeroref.jpgstreamstore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.store.EventStore;
import org.zeroref.jpgstreamstore.store.EventStoreAppendException;
import org.zeroref.jpgstreamstore.store.EventStoreException;
import org.zeroref.jpgstreamstore.store.StoreRecord;
import org.zeroref.jpgstreamstore.stream.DefaultEventStream;
import org.zeroref.jpgstreamstore.stream.EventStream;
import org.zeroref.jpgstreamstore.stream.ExpectedVersion;
import org.zeroref.jpgstreamstore.stream.StreamId;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PgEventStore implements EventStore {

    private DataSource dataSource;
    private Gson serializer = new GsonBuilder().create();


    public PgEventStore(String connectionString) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(connectionString);
        this.dataSource = ds;
    }

    public PgEventStore(DataSource aDataSource) {
        this.dataSource = aDataSource;
    }

    @Override
    public void appendToStream(StreamId streamId, List<EventData> anEvents) {
        appendToStream(streamId, ExpectedVersion.Any, anEvents);
    }

    @Override
    public void appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents) {

        try (Connection connection = this.connection()) {

            connection.setAutoCommit(false);

            int index = 0;

            for (EventData event : anEvents) {
                this.appendEventStore(connection, streamId, index++, event, expectedVersion);
            }

            connection.commit();

        } catch (Throwable t1) {
            try {
                this.connection().rollback();
            } catch (Throwable t2) {
                // ignore
            }

            throw new EventStoreAppendException(
                    "Could not append to event store because: "
                            + t1.getMessage(),
                    t1);
        }
    }

    public List<StreamId> listStreams() {
        try (Connection connection = this.connection()) {

            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "select stream_name, max(stream_version) from jpg_stream_store_log group by stream_name ");

            try (ResultSet result = statement.executeQuery()) {
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
        try (Connection connection = this.connection()) {

            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT event_id, event_body, stream_name, stream_version FROM jpg_stream_store_log "
                                            + "WHERE event_id > ? "
                                            + "ORDER BY event_id");

            statement.setLong(1, position);

            try (ResultSet result = statement.executeQuery()) {
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

        try (Connection connection = this.connection()) {

            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT stream_version, event_body FROM jpg_stream_store_log "
                                            + "WHERE stream_name = ? AND stream_version >= ? "
                                            + "ORDER BY stream_version");

            statement.setString(1, anIdentity.streamName());
            statement.setInt(2, version);

            try (ResultSet result = statement.executeQuery()) {
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
        try (Connection connection = this.connection()) {
            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT stream_version, event_body FROM jpg_stream_store_log "
                                            + "WHERE stream_name = ? "
                                            + "ORDER BY stream_version");

            statement.setString(1, anIdentity.streamName());

            try (ResultSet result = statement.executeQuery()) {
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
        try (Connection connection = this.connection()) {
            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "delete from jpg_stream_store_log "
                                            + "WHERE stream_name = ? ");

            statement.setString(1, anIdentity.streamName());
            statement.execute();

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    public void purge() {
        try (Connection connection = this.connection()) {
            connection.createStatement().execute("delete from jpg_stream_store_log");

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    public void createSchema() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("schema.sql");
        File file = new File(resource.getFile());
        String content = new String(Files.readAllBytes(file.toPath()));

        try (Connection connection = this.connection()) {
            connection.createStatement().execute(content);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        }
    }

    private void appendEventStore(
            Connection aConnection,
            StreamId anIdentity,
            int anIndex,
            EventData aEventData, int expectedVersion)
            throws Exception {

        PreparedStatement ps;

        if (expectedVersion == ExpectedVersion.Any) {
            ps = aConnection
                    .prepareStatement(
                            "INSERT INTO jpg_stream_store_log " +
                                    "(event_body, stream_name, stream_version)" +
                                    "VALUES(?, ?, (select coalesce(max(stream_version+1),1) from jpg_stream_store_log where stream_name = ?) )");

            ps.setString(1, serializer.toJson(aEventData.getProps()));
            ps.setString(2, anIdentity.streamName());
            ps.setString(3, anIdentity.streamName());
        } else {
            ps = aConnection
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
    private List<StreamId> buildEventStreamIds(ResultSet aResultSet) throws SQLException {
        List<StreamId> events = new ArrayList<>();

        while (aResultSet.next()) {
            String streamName = aResultSet.getString("stream_name");
            events.add(new StreamId(streamName));
        }

        return events;
    }

    @SuppressWarnings("unchecked")
    private List<StoreRecord> buildEventSequence(ResultSet aResultSet) throws Exception {

        List<StoreRecord> events = new ArrayList<>();

        while (aResultSet.next()) {
            long eventId = aResultSet.getLong("event_id");
            String eventBody = aResultSet.getString("event_body");
            String streamName = aResultSet.getString("stream_name");
            int streamVersion = aResultSet.getInt("stream_version");

            events.add(new StoreRecord(eventId, eventBody, streamName, streamVersion));
        }

        return events;
    }

    @SuppressWarnings("unchecked")
    private EventStream buildEventStream(ResultSet aResultSet) throws Exception {

        List<EventData> events = new ArrayList<>();

        int version = 0;

        while (aResultSet.next()) {
            version = aResultSet.getInt("stream_version");
            String eventBody = aResultSet.getString("event_body");

            Type eventClass = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> eventData = serializer.fromJson(eventBody, eventClass);

            events.add(new EventData(UUID.randomUUID(), eventData));
        }

        return new DefaultEventStream(events, version);
    }

    private Connection connection() {
        Connection connection;

        try {
            connection = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot acquire database connection.");
        }

        return connection;
    }
}
