package org.zeroref.jpgstreamstore;

import org.zeroref.jpgstreamstore.EventData;

import java.util.List;

public interface EventStream {

    public List<EventData> events();

    public int version();
}
