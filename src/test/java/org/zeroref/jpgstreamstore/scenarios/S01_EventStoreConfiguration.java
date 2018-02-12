package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.storage.PgEventStorage;

public class S01_EventStoreConfiguration {

    public static final String PG_URL = "jdbc:postgresql://localhost:5432/sqlstreamstore";

    public static void main(String[] args ){
        PgEventStorage store = new PgEventStorage(PG_URL);
    }
}
