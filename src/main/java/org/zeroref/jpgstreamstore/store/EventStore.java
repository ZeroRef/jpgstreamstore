package org.zeroref.jpgstreamstore.store;

import org.zeroref.jpgstreamstore.DomainEvent;
import org.zeroref.jpgstreamstore.stream.EventStream;
import org.zeroref.jpgstreamstore.stream.EventStreamId;

import java.util.List;

public interface EventStore {
    public void appendWith(EventStreamId aStartingIdentity, List<DomainEvent> anEvents);

    public List<DispatchableDomainEvent> eventsSince(long aLastReceivedEvent);

    public EventStream eventStreamSince(EventStreamId anIdentity);

    public EventStream fullEventStreamFor(EventStreamId anIdentity);
}
