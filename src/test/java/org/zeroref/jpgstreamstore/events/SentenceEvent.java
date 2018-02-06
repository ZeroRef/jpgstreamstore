package org.zeroref.jpgstreamstore.events;

import org.zeroref.jpgstreamstore.DomainEvent;

import java.util.Date;

public class SentenceEvent  implements DomainEvent{
    @Override
    public int eventVersion() {
        return 0;
    }

    @Override
    public Date occurredOn() {
        return new Date();
    }

    public String content = "put your stuff here";
}
