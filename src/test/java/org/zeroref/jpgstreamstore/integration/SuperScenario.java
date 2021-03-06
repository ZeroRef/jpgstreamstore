package org.zeroref.jpgstreamstore.integration;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.After;
import org.junit.Before;
import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;
import org.zeroref.jpgstreamstore.Storage;
import org.zeroref.jpgstreamstore.integration.checkers.ConnectionLeaksWatchDog;

import java.io.IOException;

public class SuperScenario {
    public static final String URL = "jdbc:postgresql://localhost:5432/jpgstreamstore";
    protected PgEventStorage store;
    protected Storage storage;
    protected Gson serializer = new GsonBuilder().create();
    private ConnectionLeaksWatchDog connectionLeaksWatchDog;

    @Before
    public void init() throws IOException {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(URL);

        storage = new Storage(dataSource);

        store = new PgEventStorage(URL);
        store.advanced().createSchema();

        connectionLeaksWatchDog = new ConnectionLeaksWatchDog(URL);
    }

    @After
    public void clean() throws Exception {
        connectionLeaksWatchDog.assertNoLeaks();
    }
}
