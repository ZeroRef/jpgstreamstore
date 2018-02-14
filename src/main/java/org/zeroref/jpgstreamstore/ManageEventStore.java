package org.zeroref.jpgstreamstore;

import java.io.IOException;
import java.util.List;

public interface ManageEventStore {

    List<StreamId> listStreams();

    List<StoreRecord> eventsSince(long position);

    void purge();

    void createSchema() throws IOException;
}
