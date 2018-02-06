package org.zeroref.jpgstreamstore.store;

public class EventStoreAppendException extends EventStoreException {

    private static final long serialVersionUID = 1;

    public EventStoreAppendException(String aMessage, Throwable aCause) {
        super(aMessage, aCause);
    }

    public EventStoreAppendException(String aMessage) {
        super(aMessage);
    }
}
