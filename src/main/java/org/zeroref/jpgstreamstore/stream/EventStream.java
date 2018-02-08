package org.zeroref.jpgstreamstore.stream;

import org.zeroref.jpgstreamstore.EventData;

import java.util.List;

public interface EventStream {

    public List<EventData> events();

    public int version();
}
