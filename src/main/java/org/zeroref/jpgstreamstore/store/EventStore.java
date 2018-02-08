package org.zeroref.jpgstreamstore.store;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.stream.EventStream;
import org.zeroref.jpgstreamstore.stream.StreamId;

import java.util.List;

public interface EventStore {
    void appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents);

    void appendToStream(StreamId streamId, List<EventData> anEvents);

    EventStream eventStreamSince(StreamId anIdentity, int version);

    EventStream fullEventStreamFor(StreamId anIdentity);

    void deleteStream(StreamId anIdentity);

    List<StoreRecord> eventsSince(long position);

}
