package org.zeroref.jpgstreamstore;

public class StoreRecord {

    private final String eventBody;
    private final String streamName;
    private final int streamVersion;
    private long eventId;

    public StoreRecord(long eventId, String eventBody, String streamName, int streamVersion) {

        this.eventId = eventId;
        this.eventBody = eventBody;
        this.streamName = streamName;
        this.streamVersion = streamVersion;
    }

    public long getEventId() {
        return eventId;
    }

    public String getEventBody() {
        return eventBody;
    }

    public String getStreamName() {
        return streamName;
    }

    public int getStreamVersion() {
        return streamVersion;
    }
}
