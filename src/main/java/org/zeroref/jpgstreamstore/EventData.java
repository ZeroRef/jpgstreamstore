package org.zeroref.jpgstreamstore;

import java.util.Map;

public class EventData {
    private Map<String, String> props;

    public EventData(Map<String, String> props) {
        this.props = props;
    }

    public Map<String, String> getProps() {
        return props;
    }
}
