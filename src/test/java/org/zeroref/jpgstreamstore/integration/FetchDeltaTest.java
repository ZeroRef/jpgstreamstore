package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.StoreRecord;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FetchDeltaTest extends SuperScenario {
    @Test
    public void fetchDelta_when_1_event_exists() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1)"
        );

        List<StoreRecord> list = store.advanced().fetchDelta(0);

        assertEquals(1, list.size());
    }

    @Test
    public void fetchDelta_will_pick_from_known_position() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D2', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1);"
        );

        List<StoreRecord> list = store.advanced().fetchDelta(1);

        assertEquals(2, list.size());
    }
}
