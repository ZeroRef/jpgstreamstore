package org.zeroref.jpgstreamstore.integration;

import com.google.gson.reflect.TypeToken;
import org.junit.Test;
import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.events.RndEventData;
import org.zeroref.jpgstreamstore.EventStoreAppendException;
import org.zeroref.jpgstreamstore.AppendResult;
import org.zeroref.jpgstreamstore.ExpectedVersion;
import org.zeroref.jpgstreamstore.StreamId;

import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class AppendToStreamTest extends SuperScenario {

    private StreamId stream1 = new StreamId("D1");
    private StreamId stream2 = new StreamId("D2");

    @Test
    public void append_first_event_will_have_version_1() {
        AppendResult result = store
                .appendToStream(stream1, 0, events(new RndEventData()));

        assertEquals(1, result.getCurrentVersion());
    }

    @Test
    public void append_1_result_in_one_record() {
        store.appendToStream(stream1, 0, events(new RndEventData()));

        assertEquals(storage.countRecords(), 1);
    }

    @Test
    public void append_first_event_will_assert_create_stream() {
        AppendResult result = store
                .appendToStream(stream1, ExpectedVersion.NoStream, events(new RndEventData()));

        assertEquals(1, result.getCurrentVersion());
    }

    @Test(expected = EventStoreAppendException.class)
    public void append_when_Expected_NoStream_will_throw() {
        store.appendToStream(stream2, ExpectedVersion.NoStream, events(new RndEventData()));
        store.appendToStream(stream2, ExpectedVersion.NoStream, events(new RndEventData()));
    }

    @Test
    public void append_two_events_will_increment_version_to_2() {
        AppendResult result = store
                .appendToStream(stream1, 0, events(new RndEventData()));

        result = store
                .appendToStream(stream1, result.getCurrentVersion(), events(new RndEventData()));

        assertEquals(2, result.getCurrentVersion());
    }

    @Test
    public void append_batch_of_two_events_will_increment_version_to_2() {
        AppendResult result = store
                .appendToStream(stream1, 0,
                        events(new RndEventData(), new RndEventData()));

        assertEquals(2, result.getCurrentVersion());
    }

    @Test
    public void append_event_at_arbitrary_version_6() {
        store.appendToStream(stream1, 6, events(new RndEventData()));

        assertEquals(storage.countRecords(), 1);
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
        store.appendToStream(stream1, 0, events(new RndEventData()));
        store.appendToStream(stream2, 0, events(new RndEventData()));

        assertEquals(storage.countRecords(), 2);
    }

    @Test
    public void content_check() {
        HashMap<String, String> contentIn = new HashMap<>();
        contentIn.put("1", "Uno");
        EventData eventData = new EventData(contentIn);

        store.appendToStream(stream1, Collections.singletonList(eventData));

        String payload = storage.listPayloads().get(0);
        Type contentClass = new TypeToken<Map<String, String>>() {
        }.getType();
        Map<String, String> contentOut = serializer.fromJson(payload, contentClass);

        assertEquals("Uno", contentOut.get("1"));
    }


    private List<EventData> events(Object... el) {
        ArrayList<EventData> result = new ArrayList<>();

        for (Object i : el)
            result.add(wrap(i));

        return result;
    }

    private EventData wrap(Object evt) {
        HashMap<String, String> props = new HashMap<>();
        props.put("type", evt.getClass().getName());
        props.put("data", serializer.toJson(evt));

        return new EventData(props);
    }
}
