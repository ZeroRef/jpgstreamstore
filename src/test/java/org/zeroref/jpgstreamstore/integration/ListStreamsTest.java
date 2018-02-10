package org.zeroref.jpgstreamstore.integration;

import org.junit.Test;
import org.zeroref.jpgstreamstore.stream.StreamId;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ListStreamsTest extends SuperScenario {
    @Test
    public void will_purge_records_from_all_streams() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1);"
        );

        List<StreamId> streamIds = store.listStreams();

        assertEquals(2, streamIds.size());
    }
}
