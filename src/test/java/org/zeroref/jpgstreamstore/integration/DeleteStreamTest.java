package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.stream.StreamId;

import static org.junit.Assert.assertEquals;

public class DeleteStreamTest extends SuperScenario {
    @Test
    public void delete_non_existing_stream_succeeds() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1)"
        );

        store.deleteStream(new StreamId("D1"));

        assertEquals(storage.countRecords(), 1);
    }

    @Test
    public void delete_will_purge_stream_records() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1);"
        );

        store.deleteStream(new StreamId("D1"));

        assertEquals(storage.countRecords(), 1);
    }
}
