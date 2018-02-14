package org.zeroref.jpgstreamstore.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.zeroref.jpgstreamstore.*;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PgEventStorage implements EventStore {
    private PgSchemaTenant tenant;
    private PgConnectionFactory connections;
    private ManageEventStore manageEventStore;
    private Gson serializer = new GsonBuilder().create();

    public PgEventStorage(String connectionString) {
        this.connections = new PgConnectionFactory(connectionString);
        this.tenant = connections.getTenant();
        this.manageEventStore = new PgManageEventStore(connections);
    }

    @Override
    public ManageEventStore advanced() {
        return manageEventStore;
    }

    @Override
    public AppendResult appendToStream(StreamId streamId, List<EventData> anEvents) {
        return appendToStream(streamId, ExpectedVersion.Any, anEvents);
    }

    @Override
    public AppendResult appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents) {

        Connection conn = connections.open();

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

        try (PreparedStatement sttmt = connection.prepareStatement(tenant.prepare(sql))) {
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

    @Override
    public EventStream fullEventStreamFor(StreamId anIdentity) {
        String sql = "SELECT stream_version, event_body FROM jpg_stream_store_log "
                + "WHERE stream_name = ? ORDER BY stream_version";

        try (Connection conn = connections.open();
             PreparedStatement sttmt = conn.prepareStatement(tenant.prepare(sql) )) {

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

        try (Connection conn = connections.open();
             PreparedStatement sttmt = conn.prepareStatement(tenant.prepare(sql))) {

            sttmt.setString(1, anIdentity.streamName());
            sttmt.execute();

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        }
    }

    @Override
    public EventStream eventStreamSince(StreamId anIdentity, int version) {

        String sql = "SELECT stream_version, event_body FROM jpg_stream_store_log "
                + "WHERE stream_name = ? AND stream_version >= ? ORDER BY stream_version";

        try (Connection conn = connections.open();
             PreparedStatement sttmt = conn.prepareStatement(tenant.prepare(sql) )) {

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

    private void appendEventStore(
            Connection conn,
            StreamId anIdentity,
            int anIndex,
            EventData aEventData, int expectedVersion)
            throws Exception {

        PreparedStatement ps;

        pack(aEventData);

        if (expectedVersion == ExpectedVersion.Any) {
            String sql = "INSERT INTO jpg_stream_store_log " +
                    "(event_body, stream_name, stream_version)" +
                    "VALUES(?, ?, (select coalesce(max(stream_version+1),1) from jpg_stream_store_log " +
                    "where stream_name = ?) )";
            ps = conn.prepareStatement(tenant.prepare(sql));

            ps.setString(1, serializer.toJson(aEventData.getHeaders()));
            ps.setString(2, anIdentity.streamName());
            ps.setString(3, anIdentity.streamName());
        } else {
            String sql = "INSERT INTO jpg_stream_store_log " +
                    "(event_body, stream_name, stream_version)" +
                    "VALUES(?, ?, ?)";
            ps = conn.prepareStatement(tenant.prepare(sql));

            ps.setString(1, serializer.toJson(aEventData.getHeaders()));
            ps.setString(2, anIdentity.streamName());
            ps.setInt(3, expectedVersion + anIndex);
        }

        ps.executeUpdate();
    }

    @SuppressWarnings("unchecked")
    private EventStream buildEventStream(ResultSet result) throws Exception {

        List<EventData> events = new ArrayList<>();

        int version = 0;

        while (result.next()) {
            version = result.getInt("stream_version");
            String eventBody = result.getString("event_body");

            EventData eventData = unpack(eventBody);
            events.add(eventData);
        }

        return new DefaultEventStream(events, version);
    }

    private void pack(EventData evt) {
        Object body = evt.getBody();
        evt.setHeader("type", body.getClass().getName());
        evt.setHeader("data", serializer.toJson(body));
    }

    private EventData unpack(String body) {
        Type headersMap = new TypeToken<Map<String, String>>() {
        }.getType();
        Map<String, String> headers = serializer.fromJson(body, headersMap);

        String data = headers.get("data");
        String type = headers.get("type");

        Class<Object> eventClass;
        try {
            eventClass = (Class<Object>) Class.forName(type);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();

            throw new IllegalStateException("Unable to load type: " + type);
        }

        Object typedInstance = serializer.fromJson(data, eventClass);

        return new EventData(headers, typedInstance);
    }
}
