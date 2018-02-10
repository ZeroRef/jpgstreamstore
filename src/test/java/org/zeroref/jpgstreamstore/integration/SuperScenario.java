package org.zeroref.jpgstreamstore.integration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.PgEventStore;
import org.zeroref.jpgstreamstore.Storage;

import java.io.IOException;

public class SuperScenario {
    protected PgEventStore store;
    protected Storage storage;
    protected Gson serializer = new GsonBuilder().create();


/*
    @Rule
    public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

    @Before
    public void init() throws IOException {
        DataSource dataSource = pg.getEmbeddedPostgres().getPostgresDatabase();
        storage = new Storage(dataSource);

        store = new PgEventStore(dataSource);
        store.createSchema();
    }
*/

    @Before
    public void init() throws IOException {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:5432/sqlstreamstore");

        storage = new Storage(dataSource);

        store = new PgEventStore(dataSource);
        store.createSchema();
    }
}
