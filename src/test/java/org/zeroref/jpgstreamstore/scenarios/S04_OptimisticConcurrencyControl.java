package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;
import org.zeroref.jpgstreamstore.StreamId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class S04_OptimisticConcurrencyControl {
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/sqlstreamstore";

    public static void main(String[] args ) throws IOException {
        PgEventStorage store = new PgEventStorage(PG_URL);
        store.createSchema();

        StreamId streamId = new StreamId("user/1");

        store.appendToStream(streamId, 1,newEvt());

        // the first append sets version 1
        // change expected version to 2 to fix the conflict
        store.appendToStream(streamId, 1,newEvt());
    }

    private static List<EventData> newEvt() {
        EventData eventData = new EventData();
        eventData.set("data", new Date().toString());
        return Arrays.asList(eventData);
    }
}
