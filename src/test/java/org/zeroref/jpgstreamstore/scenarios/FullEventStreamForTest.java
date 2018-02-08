package org.zeroref.jpgstreamstore.scenarios;

import org.junit.Test;
import org.zeroref.jpgstreamstore.stream.EventStream;
import org.zeroref.jpgstreamstore.stream.StreamId;

import static org.junit.Assert.assertEquals;

public class FullEventStreamForTest extends SuperScenario {
    @Test
    public void reads_all_events_from_stream() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D1', 3);"
        );

        EventStream stream = store.fullEventStreamFor(new StreamId("D1"));

        assertEquals(3, stream.events().size());
    }
}
