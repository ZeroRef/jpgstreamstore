package org.zeroref.jpgstreamstore;

import java.util.Map;
import java.util.UUID;

public class EventData {
    private UUID id;
    private Map<String, String> props;

    public EventData(UUID id, Map<String, String> props) {
        this.id = id;
        this.props = props;
    }

    public UUID getId() {
        return id;
    }

    public Map<String, String> getProps() {
        return props;
    }
}
