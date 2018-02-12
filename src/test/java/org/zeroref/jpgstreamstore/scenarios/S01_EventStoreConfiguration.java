package org.zeroref.jpgstreamstore.scenarios;

import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.PgEventStore;

public class S01_EventStoreConfiguration {

    public static final String PG_URL = "jdbc:postgresql://localhost:5432/sqlstreamstore";

    public static void main(String[] args ){
        option1_configureWithExternalDataSource();
        option2_configureUsingConnectionString();
    }

    private static void option1_configureWithExternalDataSource() {
        PGSimpleDataSource extDs = new PGSimpleDataSource();
        extDs.setUrl(PG_URL);

        PgEventStore store = new PgEventStore(extDs);
    }

    private static void option2_configureUsingConnectionString() {
        PgEventStore store = new PgEventStore(PG_URL);
    }
}
