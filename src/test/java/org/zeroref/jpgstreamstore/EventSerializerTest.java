package org.zeroref.jpgstreamstore;

import org.junit.Test;
import org.zeroref.jpgstreamstore.events.SentenceEvent;
import org.zeroref.jpgstreamstore.serialization.EventSerializer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventSerializerTest {
    EventSerializer serializer = new EventSerializer();
    String jsonContent = "{\"content\":\"put your stuff here\"}";

    @Test
    public void serialize_event_fields()  {
        String serialize = serializer.serialize(new SentenceEvent());
        assertThat(serialize, is(equalTo(jsonContent)));
    }

    @Test
    public void deserialize_event_fields()  {
        SentenceEvent evt = serializer.deserialize(jsonContent, SentenceEvent.class);
        assertThat(evt.content, is(equalTo("put your stuff here")));
    }
}
