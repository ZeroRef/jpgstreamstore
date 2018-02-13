package org.zeroref.jpgstreamstore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventData {
    private Map<String, String> headers = new HashMap<>();
    private Object body;

    public EventData(Object body) {
        this(new HashMap<>(), body);
    }

    public EventData(Map<String, String> headers, Object body) {
        this.headers = headers;
        this.body = body;
    }

    public String getHeader(String key){
        return headers.get(key);
    }

    public void setHeader(String key, String value){
        headers.put(key, value);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> iter = headers.entrySet().iterator();
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
