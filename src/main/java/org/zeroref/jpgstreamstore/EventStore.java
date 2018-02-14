package org.zeroref.jpgstreamstore;

import java.util.List;

public interface EventStore {
    AppendResult appendToStream(StreamId streamId, int expectedVersion, List<EventData> anEvents);

    AppendResult appendToStream(StreamId streamId, List<EventData> anEvents);

    void deleteStream(StreamId anIdentity);

    EventStream fullEventStreamFor(StreamId anIdentity);

    EventStream eventStreamSince(StreamId anIdentity, int version);

    ManageEventStore advanced();
}
