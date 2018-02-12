package org.zeroref.jpgstreamstore.storage;

public class PgSchemaTenant {
    private String currentSchema;

    public PgSchemaTenant(String currentSchema) {
        this.currentSchema = currentSchema;
    }

    public String prepare(String sqlStatement){
        if(currentSchema == null)
            return sqlStatement;

        return sqlStatement.replace("jpg_stream_store_log",
                currentSchema + "." + "jpg_stream_store_log");
    }
}
