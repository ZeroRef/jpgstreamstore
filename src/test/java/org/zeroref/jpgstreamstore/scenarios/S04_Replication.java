package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.PgEventStore;
import org.zeroref.jpgstreamstore.store.StoreRecord;
import org.zeroref.jpgstreamstore.stream.StreamId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class S04_Replication {
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/sqlstreamstore";

    public static void main(String[] args ) throws Exception {
        PgEventStore store = new PgEventStore(PG_URL);
        store.createSchema();

        try(Replication replication = new Replication(store)){
            Thread.sleep(1000);

            simulate1append(store);

            Thread.sleep(1000);

            simulate1append(store);

            Thread.sleep(2000);
        }
    }

    static class Replication implements Closeable{
        private PgEventStore store;
        private Thread runner;

        Replication(PgEventStore store) {
            this.store = store;

            runner = new Thread(() -> {
                long checkpoint = 0; // <-------- needs to be persistent for failure recovery

                System.out.println(" Replication started");
                while (true){
                    try {
                        Thread.sleep(1000);

                        System.out.println(" Checkpoint " + checkpoint);

                        List<StoreRecord> delta = store.eventsSince(checkpoint);

                        if(delta.size() == 0)
                            continue;

                        shipToDrSite(checkpoint, delta);

                        checkpoint = delta.stream()
                                .max(Comparator.comparingLong(StoreRecord::getEventId))
                                .get().getEventId();

                    } catch (InterruptedException e) {
                        break;
                    }
                }
                System.out.println(" Replication stopped");
            });

            runner.start();
        }

        private void shipToDrSite(long checkpoint, List<StoreRecord> delta) {
            System.out.println(" Ship to DR Site [Checkpoint=" + checkpoint + ", Delta=" + delta.size() + "]");
        }

        @Override
        public void close() throws IOException {
            runner.interrupt();
        }
    }



    private static void simulate1append(PgEventStore store) {
        StreamId streamId = new StreamId("user/1");
        EventData eventData = new EventData();
        eventData.set("type", "human-dna");
        eventData.set("data", "423423423493487293749287492");

        store.appendToStream(streamId, Arrays.asList(eventData));
    }
}
