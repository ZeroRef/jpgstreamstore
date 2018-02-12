package org.zeroref.jpgstreamstore.integration.checkers;

import java.sql.*;

public class ConnectionLeaksWatchDog {

    private int connectionLeakCount;
    private String url;


    public ConnectionLeaksWatchDog(String url) {
        this.url = url;

        connectionLeakCount = countConnectionLeaks();
    }

    public void assertNoLeaks() throws Exception {
        int currentConnectionLeakCount = countConnectionLeaks();
        int diff = currentConnectionLeakCount - connectionLeakCount;
        if ( diff > 0 ) {
            throw new Exception(
                    String.format(
                            "%d connection(s) have been leaked! Previous leak count: %d, Current leak count: %d",
                            diff,
                            connectionLeakCount,
                            currentConnectionLeakCount
                    )
            );
        }
    }

    private int countConnectionLeaks() {
        try ( Connection connection = newConnection() ) {
            try ( Statement statement = connection.createStatement() ) {
                try ( ResultSet resultSet = statement.executeQuery(
                        "SELECT COUNT(*) " +
                                "FROM pg_stat_activity " +
                                "WHERE state ILIKE '%idle%'" ) ) {
                    while ( resultSet.next() ) {
                        return resultSet.getInt( 1 );
                    }
                    return 0;
                }
            }
            catch ( SQLException e ) {
                throw new IllegalStateException( e );
            }
        }
        catch ( SQLException e ) {
            throw new IllegalStateException( e );
        }
    }

    private Connection newConnection() {
        try {
            return DriverManager.getConnection(url);
        }
        catch ( SQLException e ) {
            throw new IllegalStateException( e );
        }
    }
}
