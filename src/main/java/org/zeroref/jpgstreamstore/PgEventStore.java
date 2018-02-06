package org.zeroref.jpgstreamstore;

import org.zeroref.jpgstreamstore.store.DispatchableDomainEvent;
import org.zeroref.jpgstreamstore.store.EventStore;
import org.zeroref.jpgstreamstore.store.EventStoreAppendException;
import org.zeroref.jpgstreamstore.store.EventStoreException;
import org.zeroref.jpgstreamstore.stream.DefaultEventStream;
import org.zeroref.jpgstreamstore.stream.EventStream;
import org.zeroref.jpgstreamstore.stream.EventStreamId;
import org.zeroref.jpgstreamstore.serialization.EventSerializer;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PgEventStore implements EventStore {

    private DataSource dataSource;
    private EventSerializer serializer;

    public PgEventStore(DataSource aDataSource) {
        this.dataSource = aDataSource;
        this.serializer = new EventSerializer();
    }

    @Override
    public void appendWith(EventStreamId aStartingIdentity, List<DomainEvent> anEvents) {

        Connection connection = this.connection();

        try {

            connection.setAutoCommit(false);

            int index = 0;

            for (DomainEvent event : anEvents) {
                this.appendEventStore(connection, aStartingIdentity, index++, event);
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
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Override
    public List<DispatchableDomainEvent> eventsSince(long aLastReceivedEvent) {

        Connection connection = this.connection();

        ResultSet result = null;

        try {
            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT event_id, event_body, event_type FROM tbl_es_event_store "
                                            + "WHERE event_id > ? "
                                            + "ORDER BY event_id");

            statement.setLong(1, aLastReceivedEvent);

            result = statement.executeQuery();

            List<DispatchableDomainEvent> sequence = this.buildEventSequence(result);

            connection.commit();

            return sequence;

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query event for sequence since: "
                            + aLastReceivedEvent
                            + " because: "
                            + t.getMessage(),
                    t);
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Override
    public EventStream eventStreamSince(EventStreamId anIdentity) {

        Connection connection = this.connection();

        ResultSet result = null;

        try {
            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT stream_version, event_type, event_body FROM tbl_es_event_store "
                                            + "WHERE stream_name = ? AND stream_version >= ? "
                                            + "ORDER BY stream_version");

            statement.setString(1, anIdentity.streamName());
            statement.setInt(2, anIdentity.streamVersion());

            result = statement.executeQuery();

            EventStream eventStream = this.buildEventStream(result);

            if (eventStream.version() == 0) {
                throw new EventStoreException(
                        "There is no such event stream: "
                                + anIdentity.streamName()
                                + " : "
                                + anIdentity.streamVersion());
            }

            connection.commit();

            return eventStream;

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query event stream for: "
                            + anIdentity.streamName()
                            + " since version: "
                            + anIdentity.streamVersion()
                            + " because: "
                            + t.getMessage(),
                    t);
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    @Override
    public EventStream fullEventStreamFor(EventStreamId anIdentity) {

        Connection connection = this.connection();

        ResultSet result = null;

        try {
            PreparedStatement statement =
                    connection
                            .prepareStatement(
                                    "SELECT stream_version, event_type, event_body FROM tbl_es_event_store "
                                            + "WHERE stream_name = ? "
                                            + "ORDER BY stream_version");

            statement.setString(1, anIdentity.streamName());

            result = statement.executeQuery();
            connection.commit();

            return this.buildEventStream(result);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Cannot query full event stream for: "
                            + anIdentity.streamName()
                            + " because: "
                            + t.getMessage(),
                    t);
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public void purge() {
        Connection connection = this.connection();

        try {
            connection.createStatement().execute("delete from tbl_es_event_store");
            connection.commit();

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem purging event store because: "
                            + t.getMessage(),
                    t);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public void createSchema() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("schema.sql").getFile());
        String content = new String(Files.readAllBytes(file.toPath()));

        Connection connection = this.connection();

        try {
            connection.createStatement().execute(content);

        } catch (Throwable t) {
            throw new EventStoreException(
                    "Problem creating event store schema because: "
                            + t.getMessage(),
                    t);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void appendEventStore(
            Connection aConnection,
            EventStreamId anIdentity,
            int anIndex,
            DomainEvent aDomainEvent)
            throws Exception {

        PreparedStatement statement =
                aConnection
                        .prepareStatement(
                                "INSERT INTO tbl_es_event_store " +
                                        "(event_body, event_type, stream_name, stream_version)" +
                                        "VALUES(?::JSONB, ?, ?, ?)");

        statement.setString(1, serializer.serialize(aDomainEvent));
        statement.setString(2, aDomainEvent.getClass().getName());
        statement.setString(3, anIdentity.streamName());
        statement.setInt(4, anIdentity.streamVersion() + anIndex);

        statement.executeUpdate();
    }

    @SuppressWarnings("unchecked")
    private List<DispatchableDomainEvent> buildEventSequence(ResultSet aResultSet) throws Exception {

        List<DispatchableDomainEvent> events = new ArrayList<DispatchableDomainEvent>();

        while (aResultSet.next()) {
            long eventId = aResultSet.getLong("event_id");
            String eventClassName = aResultSet.getString("event_type");
            String eventBody = aResultSet.getString("event_body");

            Class<DomainEvent> eventClass = (Class<DomainEvent>) Class.forName(eventClassName);
            DomainEvent domainEvent = serializer.deserialize(eventBody, eventClass);

            events.add(new DispatchableDomainEvent(eventId, domainEvent));
        }

        return events;
    }

    @SuppressWarnings("unchecked")
    private EventStream buildEventStream(ResultSet aResultSet) throws Exception {

        List<DomainEvent> events = new ArrayList<DomainEvent>();

        int version = 0;

        while (aResultSet.next()) {
            version = aResultSet.getInt("stream_version");
            String eventClassName = aResultSet.getString("event_type");
            String eventBody = aResultSet.getString("event_body");

            Class<DomainEvent> eventClass = (Class<DomainEvent>) Class.forName(eventClassName);
            DomainEvent domainEvent = serializer.deserialize(eventBody, eventClass);

            events.add(domainEvent);
        }

        return new DefaultEventStream(events, version);
    }

    private Connection connection() {
        Connection connection = null;

        try {
            connection = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot acquire database connection.");
        }

        return connection;
    }
}
