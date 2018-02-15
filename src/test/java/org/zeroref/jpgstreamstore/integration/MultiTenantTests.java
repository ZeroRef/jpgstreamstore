package org.zeroref.jpgstreamstore.integration;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.*;
import org.zeroref.jpgstreamstore.events.RndEventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MultiTenantTests {
    public static final String URL = "jdbc:postgresql://localhost:5432/jpgstreamstore?currentSchema=tenant01";
    protected PgEventStorage store;
    protected Storage storage;

    @Before
    public void init() throws IOException {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(URL);

        storage = new Storage(dataSource);

        store = new PgEventStorage(URL);
        store.advanced().createSchema();
    }

    @Test
    public void append_for_tenant() {
        List<EventData> eventData = Arrays.asList(new EventData(new RndEventData()));
        store.appendToStream(new StreamId("a"), 0, eventData);

        assertEquals(1, storage.countRecords());
    }

    @Test
    public void delete_for_tenant() {
        storage.state(
                "INSERT INTO tenant01.jpg_stream_store_log VALUES(3, '{}', 'D3', 1)"
        );

        store.deleteStream(new StreamId("D3"));

        assertEquals(0, storage.countRecords());
    }

    @Test
    public void eventsSince_for_tenant() {
        storage.state(
                "INSERT INTO tenant01.jpg_stream_store_log VALUES(3, '{}', 'D3', 1)"
        );

        List<StoreRecord> list = store.advanced().fetchDelta(0);

        assertEquals(1, list.size());
    }

    @Test
    public void eventStreamSince_for_tenant() {
        storage.state(
                "INSERT INTO tenant01.jpg_stream_store_log VALUES(1, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 1);"
        );

        EventStream stream = store.eventStreamSince(new StreamId("D1"), 1);

        assertThat(stream.events().size(), is(equalTo(1)));
    }

    @Test
    public void fullEventStreamFor_got_tenant() {
        storage.state(
                "INSERT INTO tenant01.jpg_stream_store_log VALUES(1, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 1);"
        );

        EventStream stream = store.fullEventStreamFor(new StreamId("D1"));

        assertEquals(1, stream.events().size());
    }

    @Test
    public void listStreams_for_tenant() {
        storage.state(
                "INSERT INTO tenant01.jpg_stream_store_log VALUES(1, '{}', 'D1', 1);"
        );

        List<StreamId> streamIds = store.advanced().listStreams();

        assertEquals(1, streamIds.size());
    }
}
