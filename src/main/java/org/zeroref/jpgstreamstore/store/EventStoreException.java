package org.zeroref.jpgstreamstore.store;

public class EventStoreException extends RuntimeException {

    public EventStoreException(String aMessage) {
        super(aMessage);
    }

    public EventStoreException(String aMessage, Throwable aCause) {
        super(aMessage, aCause);
    }
}
