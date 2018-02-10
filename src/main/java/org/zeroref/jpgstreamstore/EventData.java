package org.zeroref.jpgstreamstore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventData {
    private Map<String, String> props;

    public EventData() {
        this(new HashMap<>());
    }

    public EventData(Map<String, String> props) {
        this.props = props;
    }

    public String get(String key){
        return props.get(key);
    }

    public void set(String key, String value){
        props.put(key, value);
    }

    public Map<String, String> getProps() {
        return props;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> iter = props.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            sb.append(entry.getKey());
            sb.append('=').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        return sb.toString();

    }
}
