package org.zeroref.jpgstreamstore.stream;

import org.zeroref.jpgstreamstore.DomainEvent;

import java.util.List;

public interface EventStream {

    public List<DomainEvent> events();

    public int version();
}
