package org.zeroref.jpgstreamstore.scenarios;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PurgeTest extends SuperScenario {
    @Test
    public void will_purge_records_from_all_streams() {
        storage.state(
                "INSERT INTO jpg_stream_store_log VALUES(1, '{}', 'D1', 1);" +
                        "INSERT INTO jpg_stream_store_log VALUES(2, '{}', 'D1', 2);" +
                        "INSERT INTO jpg_stream_store_log VALUES(3, '{}', 'D3', 1);"
        );

        store.purge();

        assertThat(storage.countRecords(), is(equalTo(0)));
    }
}
