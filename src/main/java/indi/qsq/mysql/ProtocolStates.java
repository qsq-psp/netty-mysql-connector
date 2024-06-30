package indi.qsq.mysql;

/**
 * Created on 2024/6/16.
 * Values smaller than zero are non-blocking states
 */
public interface ProtocolStates extends ServerCommands {

    int HANDSHAKE = -3;
    int AUTH = -2;
    int IDLE = -1;

    int QUERY_COLUMNS = 301;
    int QUERY_ROWS = 302;
    int STATEMENT_PREPARE_COLUMNS = 2201;
    int STATEMENT_EXECUTE_COLUMNS = 2301;
    int STATEMENT_EXECUTE_ROWS = 2302;

    int SYN = 10001;
    int SSL = 10002;
}
