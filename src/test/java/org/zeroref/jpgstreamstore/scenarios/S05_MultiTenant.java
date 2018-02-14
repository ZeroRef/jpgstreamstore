package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.StreamId;
import org.zeroref.jpgstreamstore.events.RndEventData;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;

import java.io.IOException;
import java.util.Arrays;

public class S05_MultiTenant {

    public static void main(String[] args ) throws IOException {
        PgEventStorage auditStore = new PgEventStorage(
                "jdbc:postgresql://localhost:5432/jpgstreamstore?currentSchema=audit");
        auditStore.advanced().createSchema();

        PgEventStorage billingStore = new PgEventStorage(
                "jdbc:postgresql://localhost:5432/jpgstreamstore?currentSchema=billing");
        billingStore.advanced().createSchema();



        EventData auditR = new EventData(new RndEventData());
        auditR.setHeader("action", "authorized-transfer-10000");

        auditStore.appendToStream(new StreamId("user/1"), Arrays.asList(auditR));


        EventData transferR = new EventData(new RndEventData());
        transferR.setHeader("transaction", "from-ac100-to-ac300-send-3000USD");

        billingStore.appendToStream(new StreamId("user/1"), Arrays.asList(transferR));
    }
}
