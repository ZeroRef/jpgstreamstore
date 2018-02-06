package org.zeroref.jpgstreamstore;

import org.zeroref.jpgstreamstore.events.RndEvent;
import org.zeroref.jpgstreamstore.stream.EventStreamId;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AppendToEventStoreTest
{
    @Test
    public void append_1_event()  {

        try(EventStoreContext context = new EventStoreContext()){
            PgEventStore store = context.getEventStore();

            EventStreamId streamD1 = new EventStreamId("D1");
            List<DomainEvent> events1 = Arrays.asList(new RndEvent());

            store.appendWith(streamD1, events1);

            assertThat(context.countRecords(), is(equalTo(1)));
        }
    }

    @Test
    public void append_transactional_set_of_2_event()  {

        try(EventStoreContext context = new EventStoreContext()){
            PgEventStore store = context.getEventStore();

            EventStreamId streamD1 = new EventStreamId("D2");
            List<DomainEvent> events1 = Arrays.asList(new RndEvent(), new RndEvent());

            store.appendWith(streamD1, events1);

            assertThat(context.countRecords(), is(equalTo(2)));
        }
    }

    @Test
    public void append_2_events_seq()  {

        try(EventStoreContext context = new EventStoreContext()){
            PgEventStore store = context.getEventStore();

            store.appendWith(new EventStreamId("D2",1), Arrays.asList(new RndEvent()));
            store.appendWith(new EventStreamId("D2",2), Arrays.asList(new RndEvent()));

            assertThat(context.countRecords(), is(equalTo(2)));
        }
    }
}
