package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;
import org.zeroref.jpgstreamstore.StreamId;

import java.io.IOException;
import java.util.Arrays;

public class S02_WriteToStream {
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/jpgstreamstore";

    public static void main(String[] args ) throws IOException {
        PgEventStorage store = new PgEventStorage(PG_URL);
        store.createSchema();

        StreamId streamId = new StreamId("user/1");
        EventData eventData = new EventData();
        eventData.set("type", "human-dna");
        eventData.set("data", "423423423493487293749287492");

        store.appendToStream(streamId, Arrays.asList(eventData));
    }
}
