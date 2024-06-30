package indi.qsq.mysql;

/**
 * Created on 2024/5/21.
 * https://dev.mysql.com/doc/dev/mysql-server/latest/my__command_8h.html
 * https://dev.mysql.com/doc/dev/mysql-server/latest/my__command_8h_source.html
 * com.mysql.cj.protocol.a.NativeConstants
 */
public interface ServerCommands {

    int SLEEP = 0;
    int QUIT = 1;
    int INIT_DB = 2;
    int QUERY = 3;
    int FIELD_LIST = 4; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    int CREATE_DB = 5; // Not used; deprecated?
    int DROP_DB = 6; // Not used; deprecated?
    int REFRESH = 7; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    int SHUTDOWN = 8; // Deprecated in MySQL 5.7.9 and MySQL 8.0.0.
    int STATISTICS = 9;
    int PROCESS_INFO = 10; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    int CONNECT = 11;
    int PROCESS_KILL = 12; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    int DEBUG = 13;
    int PING = 14;
    int TIME = 15;
    int DELAYED_INSERT = 16;
    int CHANGE_USER = 17;
    int BINLOG_DUMP = 18;
    int TABLE_DUMP = 19;
    int CONNECT_OUT = 20;
    int REGISTER_SLAVE = 21;
    int STATEMENT_PREPARE = 22;
    int STATEMENT_EXECUTE = 23;
    int STATEMENT_SEND_LONG_DATA = 24; // No response
    int STATEMENT_CLOSE = 25; // No response
    int STATEMENT_RESET = 26;
    int SET_OPTION = 27;
    int STMT_FETCH = 28;
    int DAEMON = 29;
    int BINLOG_DUMP_GTID = 30;
    int RESET_CONNECTION = 31;
}
