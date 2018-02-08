package org.zeroref.jpgstreamstore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;
import org.zeroref.jpgstreamstore.events.SentenceEventData;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventSerializerTest {
    protected Gson serializer = new GsonBuilder().create();

    String jsonContent = "{\"content\":\"put your stuff here\"}";

    @Test
    public void serialize_event_fields() {
        String serialize = serializer.toJson(new SentenceEventData());
        assertThat(serialize, is(equalTo(jsonContent)));
    }

    @Test
    public void deserialize_event_fields() {
        SentenceEventData evt = serializer.fromJson(jsonContent, SentenceEventData.class);
        assertThat(evt.content, is(equalTo("put your stuff here")));
    }
}
