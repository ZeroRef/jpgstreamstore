package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.events.RndEventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;
import org.zeroref.jpgstreamstore.StoreRecord;
import org.zeroref.jpgstreamstore.StreamId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class S04_Replication {
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/jpgstreamstore";

    public static void main(String[] args ) throws Exception {
        PgEventStorage store = new PgEventStorage(PG_URL);
        store.advanced().createSchema();

        try(Replication replication = new Replication(store)){
            Thread.sleep(1000);

            simulate1append(store);

            Thread.sleep(1000);

            simulate1append(store);

            Thread.sleep(2000);
        }
    }

    static class Replication implements Closeable{
        private PgEventStorage store;
        private Thread runner;

        Replication(PgEventStorage store) {
            this.store = store;

            runner = new Thread(() -> {
                long checkpoint = 0; // <-------- needs to be persistent for failure recovery

                System.out.println(" Replication started");
                while (true){
                    try {
                        Thread.sleep(1000);

                        System.out.println(" Checkpoint " + checkpoint);

                        List<StoreRecord> delta = store.advanced().eventsSince(checkpoint);

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



    private static void simulate1append(PgEventStorage store) {
        StreamId streamId = new StreamId("user/1");
        EventData eventData = new EventData(new RndEventData());
        eventData.setHeader("type", "human-dna");
        eventData.setHeader("data", "423423423493487293749287492");

        store.appendToStream(streamId, Arrays.asList(eventData));
    }
}
