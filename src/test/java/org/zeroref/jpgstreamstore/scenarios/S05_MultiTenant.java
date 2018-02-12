package org.zeroref.jpgstreamstore.scenarios;

import org.zeroref.jpgstreamstore.EventData;
import org.zeroref.jpgstreamstore.StreamId;
import org.zeroref.jpgstreamstore.storage.PgEventStorage;

import java.io.IOException;
import java.util.Arrays;

public class S05_MultiTenant {

    public static void main(String[] args ) throws IOException {
        PgEventStorage auditStore = new PgEventStorage(
                "jdbc:postgresql://localhost:5432/sqlstreamstore?currentSchema=audit");
        auditStore.createSchema();

        PgEventStorage billingStore = new PgEventStorage(
                "jdbc:postgresql://localhost:5432/sqlstreamstore?currentSchema=billing");
        billingStore.createSchema();



        EventData auditR = new EventData();
        auditR.set("action", "authorized-transfer-10000");

        auditStore.appendToStream(new StreamId("user/1"), Arrays.asList(auditR));


        EventData transferR = new EventData();
        transferR.set("transaction", "from-ac100-to-ac300-send-3000USD");

        billingStore.appendToStream(new StreamId("user/1"), Arrays.asList(transferR));
    }
}
