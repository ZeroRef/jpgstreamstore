package org.zeroref.jpgstreamstore.scenarios;

import org.postgresql.ds.PGSimpleDataSource;
import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;
import org.zeroref.jpgstreamstore.EventStream;
import org.zeroref.jpgstreamstore.StreamId;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class S03_ReadFromStream {
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/jpgstreamstore";

    public static void main(String[] args ) throws Exception {
        PgEventStorage store = new PgEventStorage(PG_URL);
        store.createSchema();

        prepareState();


        StreamId streamId = new StreamId("user/1");
        EventStream stream = store.fullEventStreamFor(streamId);

        System.out.println("Stream name " + streamId);
        System.out.println("Stream version " + stream.version());
        System.out.println("Stream events " + stream.events().size());

        for (EventData e : stream.events()){
            System.out.println("\t " + e);
        }
    }

    private static void prepareState() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG_URL);

        try(Connection connection = ds.getConnection();
            Statement statement = connection.createStatement()
        ){
            String sql = "INSERT INTO jpg_stream_store_log VALUES(1, '{\"type\":\"human-dna\"}', 'user/1', 1)";
            statement.execute(sql);
        }
    }
}
