package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.EventStoreException;
import org.zeroref.jpgstreamstore.EventStream;
import org.zeroref.jpgstreamstore.StreamId;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventStreamSinceTest extends SuperScenario {

    @Test
    public void will_pick_from_known_position() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D1', 3);"
        );

        EventStream stream = store.eventStreamSince(new StreamId("D1"), 2);

        assertThat(stream.events().size(), is(equalTo(2)));
    }

    @Test(expected = EventStoreException.class)
    public void will_throw_when_no_stream_exists() {
        store.eventStreamSince(new StreamId("D2"), 1);
    }
}
