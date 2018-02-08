package org.zeroref.jpgstreamstore.scenarios;

import org.junit.Test;
import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.events.RndEventData;
import org.zeroref.jpgstreamstore.store.EventStoreAppendException;
import org.zeroref.jpgstreamstore.stream.ExpectedVersion;
import org.zeroref.jpgstreamstore.stream.StreamId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AppendToStreamTest extends SuperScenario {

    StreamId stream1 = new StreamId("D1");
    StreamId stream2 = new StreamId("D2");

    @Test
    public void append_1_event() {
        List<EventData> events1 = events(new RndEventData());

        store.appendToStream(stream1, 1, events1);

        assertEquals(storage.countRecords(), 1);
    }

    @Test
    public void append_transactional_set_of_2_event() {
        List<EventData> events1 = events(new RndEventData(), new RndEventData());

        store.appendToStream(stream1, 1, events1);

        assertEquals(storage.countRecords(), 2);
    }

    @Test
    public void append_2_events_seq() {
        store.appendToStream(stream2, 1, events(new RndEventData()));
        store.appendToStream(stream2, 2, events(new RndEventData()));

        assertEquals(storage.countRecords(), 2);
    }

    @Test
    public void append_2_events_any() {
        store.appendToStream(stream2, ExpectedVersion.Any, events(new RndEventData()));
        store.appendToStream(stream2, ExpectedVersion.Any, events(new RndEventData()));

        assertEquals(storage.countRecords(), 2);
    }

    @Test
    public void append_2_events_defaults_to_any() {
        store.appendToStream(stream2, events(new RndEventData()));
        store.appendToStream(stream2, events(new RndEventData()));

        assertEquals(storage.countRecords(), 2);
    }

    @Test(expected = EventStoreAppendException.class)
    public void append_optimistic_concurrency_control() {
        store.appendToStream(stream2, 1, events(new RndEventData()));
        store.appendToStream(stream2, 1, events(new RndEventData()));
    }

    @Test
    public void append_to_2_different_streams() {
        store.appendToStream(stream1, 1, events(new RndEventData()));
        store.appendToStream(stream2, 1, events(new RndEventData()));

        assertEquals(storage.countRecords(), 2);
    }

    List<EventData> events(Object... el) {
        ArrayList<EventData> result = new ArrayList<>();

        for (Object i : el)
            result.add(wrap(i));

        return result;
    }

    EventData wrap(Object evt) {
        UUID id = UUID.randomUUID();

        HashMap<String, String> props = new HashMap<>();
        props.put("type", evt.getClass().getName());
        props.put("data", serializer.toJson(evt));

        return new EventData(id, props);
    }
}
