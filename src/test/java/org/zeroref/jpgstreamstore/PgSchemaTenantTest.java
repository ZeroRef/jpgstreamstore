package org.zeroref.jpgstreamstore;

import org.junit.Test;
import org.zeroref.jpgstreamstore.storage.PgSchemaTenant;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PgSchemaTenantTest {
    @Test
    public void swap_schema() {

        PgSchemaTenant tenant = new PgSchemaTenant("audit");

        String serialize = tenant.prepare("delete from jpg_stream_store_log");
        assertThat(serialize, is(equalTo("delete from audit.jpg_stream_store_log")));
    }

    @Test
    public void nothing_for_no_schema() {

        PgSchemaTenant tenant = new PgSchemaTenant(null);

        String serialize = tenant.prepare("delete from jpg_stream_store_log");
        assertThat(serialize, is(equalTo("delete from jpg_stream_store_log")));
    }
}
