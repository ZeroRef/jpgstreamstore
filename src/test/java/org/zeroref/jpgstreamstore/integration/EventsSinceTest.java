package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.EventData;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EventsSinceTest extends SuperScenario {
    @Test
    public void eventsSince_when_1_event_exists() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(3, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D3', 1)"
        );

        List<EventData> list = store.eventsSince(0);

        assertEquals(1, list.size());
        assertNotNull(list.get(0));
    }

    @Test
    public void eventsSince_will_pick_from_known_position() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D2', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{\"data\":\"{}\",\"type\":\"java.lang.Object\"}', 'D3', 1);"
        );

        List<EventData> list = store.eventsSince(1);

        assertEquals(2, list.size());
    }
}