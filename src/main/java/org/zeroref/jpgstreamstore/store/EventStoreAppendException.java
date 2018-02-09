package org.zeroref.jpgstreamstore.store;

public class EventStoreAppendException extends EventStoreException {

    public EventStoreAppendException(String aMessage, Throwable aCause) {
        super(aMessage, aCause);
    }

    public EventStoreAppendException(String aMessage) {
        super(aMessage);
    }
}
