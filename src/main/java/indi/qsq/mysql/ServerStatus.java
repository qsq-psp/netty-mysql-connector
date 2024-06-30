package indi.qsq.mysql;

/**
 * Created on 2024/5/20.
 *
 * https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1d854e841086925be1883e4d7b4e8cad
 */
@SuppressWarnings("PointlessBitwiseExpression")
public interface ServerStatus {

    /**
     * Is raised when a multi-statement transaction has been started, either explicitly, by means of BEGIN or COMMIT AND CHAIN, or implicitly, by the first transactional statement, when autocommit=off.
     */
    int IN_TRANSACTION = 1 << 0;

    /**
     * Server in auto_commit mode.
     */
    int AUTO_COMMIT = 1 << 1;

    /**
     * Multi query - next query exists.
     */
    int MORE_RESULTS_EXISTS = 1 << 2;

    int QUERY_NO_GOOD_INDEX_USED = 1 << 4;

    int QUERY_NO_INDEX_USED = 1 << 5;

    /**
     * The server was able to fulfill the clients request and opened a read-only non-scrollable cursor for a query.
     * This flag comes in reply to COM_STMT_EXECUTE and COM_STMT_FETCH commands. Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
     */
    int CURSOR_EXISTS = 1 << 6;

    /**
     * This flag is sent when a read-only cursor is exhausted, in reply to COM_STMT_FETCH command.
     */
    int LAST_ROW_SENT = 1 << 7;

    /**
     * A database was dropped.
     */
    int DB_DROPPED = 1 << 8;

    int NO_BACKSLASH_ESCAPES = 1 << 9;

    /**
     * Sent to the client if after a prepared statement reprepare we discovered that the new statement returns a different number of result set columns.
     */
    int METADATA_CHANGED = 1 << 10;

    int QUERY_WAS_SLOW = 1 << 11;

    /**
     * To mark ResultSet containing output parameter values.
     */
    int PS_OUT_PARAMS = 1 << 12;

    /**
     * Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement transaction is a read-only transaction.
     * Cleared when the transaction commits or aborts. Since this flag is sent to clients in OK and EOF packets, the flag indicates the transaction status at the end of command execution.
     */
    int IN_TRANSACTION_READ_ONLY = 1 << 13;

    /**
     * This status flag, when on, implies that one of the state information has changed on the server because of the execution of the last statement.
     */
    int SESSION_STATE_CHANGED = 1 << 14;
}
