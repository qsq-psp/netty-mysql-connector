package indi.qsq.mysql;

/**
 * https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
 */
@SuppressWarnings("PointlessBitwiseExpression")
public interface CapabilitiesFlags {

    /**
     * Use the improved version of Old Password Authentication. Not used. Assumed to be set since 4.1.1.
     */
    int LONG_PASSWORD = 1 << 0;

    /**
     * Send found rows instead of affected rows in EOF_Packet.
     */
    int FOUND_ROWS = 1 << 1;

    /**
     * Get all column flags.
     * Longer flags in Protocol::ColumnDefinition320.
     */
    int LONG_FLAG = 1 << 2;

    /**
     * Database (schema) name can be specified on connect in Handshake Response Packet.
     * Server: Supports schema-name in Handshake Response Packet.
     * Client: Handshake Response Packet contains a schema-name.
     */
    int CONNECT_WITH_DB = 1 << 3;

    /**
     * Don't allow database.table.column.
     */
    @Deprecated
    int NO_SCHEMA = 1 << 4;

    /**
     * Compression protocol supported.
     * Server: Supports compression.
     * Client: Switches to Compression compressed protocol after successful authentication.
     */
    int COMPRESS = 1 << 5;

    /**
     * Special handling of ODBC behavior. No special behavior since 3.22.
     */
    int ODBC = 1 << 6;

    /**
     * Can use LOAD DATA LOCAL.
     * Server: Enables the LOCAL INFILE request of LOAD DATA|XML.
     * Client: Will handle LOCAL INFILE request.
     */
    int LOCAL_FILES = 1 << 7;

    /**
     * Ignore spaces before '('.
     * Server: Parser can ignore spaces before '('.
     * Client: Let the parser ignore spaces before '('.
     */
    int IGNORE_SPACE = 1 << 8;

    /**
     * New 4.1 protocol. this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0.
     * Server: Supports the 4.1 protocol.
     * Client: Uses the 4.1 protocol.
     */
    int PROTOCOL_41 = 1 << 9;

    /**
     * This is an interactive client. Use System_variables::net_wait_timeout versus System_variables::net_interactive_timeout.
     * Server: Supports interactive and noninteractive clients.
     * Client: Client is interactive.
     */
    int INTERACTIVE = 1 << 10;

    /**
     * Use SSL encryption for the session.
     * Server: Supports SSL
     * Client: Switch to SSL after sending the capability-flags.
     */
    int SSL = 1 << 11;

    /**
     * Client only flag. Not used.
     * Client: Do not issue SIGPIPE if network failures occur (libmysqlclient only).
     */
    int IGNORE_SIGPIPE = 1 << 12;

    /**
     * Client knows about transactions.
     * Server: Can send status flags in OK_Packet / EOF_Packet.
     * Client: Expects status flags in OK_Packet / EOF_Packet.
     */
    int TRANSACTIONS = 1 << 13;

    @SuppressWarnings("unused")
    int RESERVED_1 = 1 << 14;

    @SuppressWarnings("unused")
    int RESERVED_2 = 1 << 15;

    /**
     * Enable/disable multi-stmt support. Also sets CLIENT_MULTI_RESULTS. Currently not checked anywhere.
     * Server: Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
     * Client: May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
     */
    int MULTI_STATEMENTS = 1 << 16;

    /**
     * Enable/disable multi-results.
     * Server: Can send multiple resultsets for COM_QUERY. Error if the server needs to send them and client does not support them.
     * Client: Can handle multiple resultsets for COM_QUERY.
     */
    int MULTI_RESULTS = 1 << 17;

    /**
     * Multi-results and OUT parameters in PS-protocol.
     * Server: Can send multiple resultsets for COM_STMT_EXECUTE.
     * Client: Can handle multiple resultsets for COM_STMT_EXECUTE.
     */
    int PS_MULTI_RESULTS = 1 << 18;

    /**
     * Client supports plugin authentication. Requires CLIENT_PROTOCOL_41.
     * Server: Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.
     * Client: Supports authentication plugins.
     */
    int PLUGIN_AUTH = 1 << 19;

    /**
     * Client supports connection attributes.
     * Server: Permits connection attributes in Protocol::HandshakeResponse41.
     * Client: Sends connection attributes in Protocol::HandshakeResponse41.
     */
    int CONNECT_ATTRS = 1 << 20;

    /**
     * Enable authentication response packet to be larger than 255 bytes.
     * When the ability to change default plugin require that the initial password field in the Protocol::HandshakeResponse41 packet can be of arbitrary size.
     * However, the 4.1 client-server protocol limits the length of the auth-data-field sent from client to server to 255 bytes.
     * The solution is to change the type of the field to a true length encoded string and indicate the protocol change with this client capability flag.
     * Server: Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.
     * Client: Length of auth response data in Protocol::HandshakeResponse41 is a length-encoded integer.
     */
    int PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;

    /**
     * Don't close the connection for a user account with expired password.
     * Server: Announces support for expired password extension.
     * Client: Can handle expired passwords.
     */
    int CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22;

    /**
     * Capable of handling server state change information. Its a hint to the server to include the state change information in OK_Packet.
     * Server: Can set SERVER_SESSION_STATE_CHANGED in the SERVER_STATUS_flags_enum and send Session State Information in a OK_Packet.
     * Client: Expects the server to send Session State Information in a OK_Packet.
     */
    int SESSION_TRACK = 1 << 23;

    /**
     * Client no longer needs EOF_Packet and will use OK_Packet instead.
     * Server: Can send OK after a Text Resultset.
     * Client: Expects an OK_Packet (instead of EOF_Packet) after the resultset rows of a Text Resultset.
     */
    int DEPRECATE_EOF = 1 << 24;

    /**
     * The client can handle optional metadata information in the resultset.
     */
    int OPTIONAL_RESULTSET_METADATA = 1 << 25;

    /**
     * Compression protocol extended to support zstd compression method.
     * This capability flag is used to send zstd compression level between client and server provided both client and server are enabled with this flag.
     * Server: Server sets this flag when global variable protocol-compression-algorithms has zstd in its list of supported values.
     * Client: Client sets this flag when it is configured to use zstd compression method.
     */
    int ZSTD_COMPRESSION_ALGORITHM = 1 << 26;

    /**
     * Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets.
     * Server: Expects an optional part containing the query parameter set(s). Executes the query for each set of parameters or returns an error if more than 1 set of parameters is sent and the server can't execute it.
     * Client: Can send the optional part containing the query parameter set(s).
     */
    int QUERY_ATTRIBUTES = 1 << 27;

    /**
     * Support Multi factor authentication.
     * Server: Server sends AuthNextFactor packet after every nth factor authentication method succeeds, except the last factor authentication.
     * Client: Client reads AuthNextFactor packet sent by server and initiates next factor authentication method.
     */
    int MULTI_FACTOR_AUTHENTICATION = 1 << 28;

    /**
     * This flag will be reserved to extend the 32bit capabilities structure to 64bits.
     */
    int CAPABILITY_EXTENSION = 1 << 29;

    /**
     * Verify server certificate. Client only flag.
     */
    int SSL_VERIFY_SERVER_CERT = 1 << 30;

    /**
     * Don't reset the options after an unsuccessful connect. Client only flag.
     * Typically passed via mysql_real_connect()'s client_flag parameter.
     */
    int REMEMBER_OPTIONS = 1 << 31;
}
