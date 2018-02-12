package org.zeroref.jpgstreamstore;

import java.util.List;

public interface EventStore {
    AppendResult appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents);

    AppendResult appendToStream(StreamId streamId, List<EventData> anEvents);

    EventStream eventStreamSince(StreamId anIdentity, int version);

    EventStream fullEventStreamFor(StreamId anIdentity);

    void deleteStream(StreamId anIdentity);

    List<StoreRecord> eventsSince(long position);

}
