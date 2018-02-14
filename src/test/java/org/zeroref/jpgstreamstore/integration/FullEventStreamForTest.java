package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.EventStream;
import org.zeroref.jpgstreamstore.StreamId;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class FullEventStreamForTest extends SuperScenario {
    @Test
    public void reads_all_events_from_stream() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 3);"
        );

        EventStream stream = store.fullEventStreamFor(new StreamId("D1"));

        List<EventData> events = stream.events();
        assertEquals(3, events.size());

        assertThat(events.get(0).getBody(), instanceOf(Object.class));
    }
}
