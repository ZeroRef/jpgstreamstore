package org.zeroref.jpgstreamstore.stream;

public final class StreamId {

    private String streamName;

    public StreamId(String aStreamName) {
        this.setStreamName(aStreamName);
    }

    public String streamName() {
        return this.streamName;
    }

    private void setStreamName(String aStreamName) {
        this.streamName = aStreamName;
    }
}
