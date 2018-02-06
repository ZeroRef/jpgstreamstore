package org.zeroref.jpgstreamstore.events;

import org.zeroref.jpgstreamstore.DomainEvent;

import java.util.Date;
import java.util.UUID;

public class RndEvent implements DomainEvent {
    public int eventVersion() {
        return 0;
    }
    public Date occurredOn() {
        return new Date();
    }

    public String uuid = UUID.randomUUID().toString();
}
