package org.zeroref.jpgstreamstore.stream;

import org.zeroref.jpgstreamstore.EventData;

import java.util.List;

public class DefaultEventStream implements EventStream {

    private List<EventData> events;
    private int version;

    public DefaultEventStream(List<EventData> anEventsList, int aVersion) {
        super();

        this.setEvents(anEventsList);
        this.setVersion(aVersion);
    }

    @Override
    public List<EventData> events() {
        return this.events;
    }

    @Override
    public int version() {
        return this.version;
    }

    private void setEvents(List<EventData> anEventsList) {
        this.events = anEventsList;
    }

    private void setVersion(int aVersion) {
        this.version = aVersion;
    }
}
