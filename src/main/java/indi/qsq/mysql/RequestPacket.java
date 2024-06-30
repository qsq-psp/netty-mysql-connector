package indi.qsq.mysql;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPromise;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2024/5/19.
 */
public abstract class RequestPacket extends Packet {

    /** Used when this packet is waiting in the queue */
    ChannelPromise writePromise;

    /** Added when this packet is ready to send */
    ChannelHandler responseHandler;

    protected boolean trySuccess() {
        return writePromise != null && writePromise.trySuccess();
    }

    protected boolean tryFailure(@NotNull Throwable cause) {
        return writePromise != null && writePromise.tryFailure(cause);
    }

    public int getCommand() {
        return ProtocolStates.IDLE;
    }

    public boolean isQuit() {
        return false;
    }

    /**
     * Created on 2024/5/18.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html
     */
    public static class SslRequest extends RequestPacket {

        static final byte[] FILLER_ZEROS = new byte[23];

        /**
         * {@link CapabilitiesFlags}
         */
        int clientCapabilitiesFlags;

        int maxPacketSize;

        /**
         * {@link CharacterSets}
         */
        int characterSet;

        @Override
        public int getCommand() {
            return ProtocolStates.SSL;
        }

        public boolean hasCapability(int flag) {
            return (clientCapabilitiesFlags & flag) != 0;
        }

        public void setCapability(int flag, boolean enabled) {
            if (enabled) {
                clientCapabilitiesFlags |= flag;
            } else {
                clientCapabilitiesFlags &= ~flag;
            }
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                clientCapabilitiesFlags = buf.readInt4();
                maxPacketSize = buf.readInt4();
                characterSet = buf.readInt1();
                buf.assertEquals(FILLER_ZEROS, buf.readFixedLengthBytes(FILLER_ZEROS.length));
            } else {
                clientCapabilitiesFlags = buf.readInt2();
                maxPacketSize = buf.readInt3();
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.writeInt4(clientCapabilitiesFlags);
                buf.writeInt4(maxPacketSize);
                buf.writeInt1(characterSet);
                buf.writeFixedLengthBytes(FILLER_ZEROS);
            } else {
                buf.writeInt2(clientCapabilitiesFlags);
                buf.writeInt3(maxPacketSize);
            }
        }
    }

    /**
     * Created on 2024/5/18.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
     */
    public static class HandshakeResponse extends SslRequest implements AuthPlugin.NameHolder, AuthPlugin.DataHolder {

        String userName;

        byte[] authResponse;

        String schemaName;

        String pluginName;

        Map<String, String> clientAttributes;

        /**
         * min = 0, max = 22
         */
        int zstdCompressionLevel;

        @Override
        public int getCommand() {
            return ProtocolStates.AUTH;
        }

        @Override
        public String getAuthPluginName() {
            return pluginName;
        }

        @Override
        public byte[] getAuthData() {
            return authResponse;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                clientCapabilitiesFlags = buf.readInt4();
                maxPacketSize = buf.readInt4();
                characterSet = buf.readInt1();
                buf.assertEquals(FILLER_ZEROS, buf.readFixedLengthBytes(FILLER_ZEROS.length));
                userName = buf.readNullTerminatedString();
                if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                    authResponse = buf.readLengthEncodedBytes();
                } else {
                    authResponse = buf.readFixedLengthBytes(buf.readInt1());
                }
                if (hasCapability(CapabilitiesFlags.CONNECT_WITH_DB)) {
                    schemaName = buf.readNullTerminatedString();
                }
                if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH)) {
                    pluginName = buf.readNullTerminatedString();
                }
                if (hasCapability(CapabilitiesFlags.CONNECT_ATTRS)) {
                    HashMap<String, String> map = new HashMap<>();
                    MysqlByteBuf subBuf = buf.readSlice(buf.readLengthEncodedInteger());
                    while (subBuf.isReadable()) {
                        map.put(
                                subBuf.readLengthEncodedString(),
                                subBuf.readLengthEncodedString()
                        );
                    }
                    clientAttributes = map;
                }
                if (hasCapability(CapabilitiesFlags.ZSTD_COMPRESSION_ALGORITHM)) {
                    zstdCompressionLevel = buf.readInt1();
                }
            } else {
                clientCapabilitiesFlags = buf.readInt2();
                maxPacketSize = buf.readInt3();
                userName = buf.readNullTerminatedString();
                if (hasCapability(CapabilitiesFlags.CONNECT_WITH_DB)) {
                    authResponse = buf.readNullTerminatedBytes();
                    schemaName = buf.readNullTerminatedString();
                } else {
                    authResponse = buf.readRestOfPacketBytes();
                }
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.writeInt4(clientCapabilitiesFlags);
                buf.writeInt4(maxPacketSize);
                buf.writeInt1(characterSet);
                buf.writeFixedLengthBytes(FILLER_ZEROS);
                buf.writeNullTerminatedString(userName);
                if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                    buf.writeLengthEncodedBytes(authResponse);
                } else {
                    buf.writeInt1(authResponse.length);
                    buf.writeFixedLengthBytes(authResponse);
                }
                if (hasCapability(CapabilitiesFlags.CONNECT_WITH_DB)) {
                    buf.writeNullTerminatedString(schemaName);
                }
                if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH)) {
                    buf.writeNullTerminatedString(pluginName);
                }
                if (hasCapability(CapabilitiesFlags.CONNECT_ATTRS)) {
                    MysqlByteBuf subBuf = buf.allocateBuffer();
                    for (Map.Entry<String, String> entry : clientAttributes.entrySet()) {
                        subBuf.writeLengthEncodedString(entry.getKey());
                        subBuf.writeLengthEncodedString(entry.getValue());
                    }
                    buf.writeLengthEncodedBytes(subBuf);
                }
                if (hasCapability(CapabilitiesFlags.ZSTD_COMPRESSION_ALGORITHM)) {
                    buf.writeInt1(zstdCompressionLevel);
                }
            } else {
                buf.writeInt2(clientCapabilitiesFlags);
                buf.writeInt3(maxPacketSize);
                buf.writeNullTerminatedString(userName);
                if (hasCapability(CapabilitiesFlags.CONNECT_WITH_DB)) {
                    buf.writeNullTerminatedBytes(authResponse);
                    buf.writeNullTerminatedString(schemaName);
                } else {
                    buf.writeFixedLengthBytes(authResponse);
                }
            }
        }
    }

    /**
     * Created on 2024/5/27.
     *
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
     * This is a request packet (from client to server)
     * Next: {@link ResponsePacket.AuthMoreData}; {@link ResponsePacket.Error} or {@link ResponsePacket.OK}
     */
    public static class AuthSwitchResponse extends RequestPacket implements AuthPlugin.DataHolder {

        byte[] authResponse;

        @Override
        public int getCommand() {
            return ProtocolStates.AUTH;
        }

        @Override
        public byte[] getAuthData() {
            return authResponse;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            authResponse = buf.readRestOfPacketBytes();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeFixedLengthBytes(authResponse);
        }

        @Override
        public String toString() {
            return String.format(
                    "AuthSwitchResponse[authResponseLength = %d]",
                    authResponse != null ? authResponse.length : -1
            );
        }
    }

    /**
     * Created on 2024/6/1.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_next_factor_request.html
     * Next Authentication method Packet in Multi Factor Authentication
     * If both server and the client support MULTI_FACTOR_AUTHENTICATION capability, server can send this packet to ask client to initiate next authentication method in Multi Factor Authentication process.
     * Next: Protocol::AuthNextFactor: or closing the connection.
     */
    public static class AuthNextFactor extends RequestPacket implements AuthPlugin.NameHolder, AuthPlugin.DataHolder {

        static final int PACKET_HEADER = 0x02;

        String pluginName;

        byte[] authData;

        @Override
        public int getCommand() {
            return ProtocolStates.AUTH;
        }

        @Override
        public String getAuthPluginName() {
            return pluginName;
        }

        @Override
        public byte[] getAuthData() {
            return authData;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PACKET_HEADER, buf.readInt1());
            pluginName = buf.readNullTerminatedString();
            authData = buf.readRestOfPacketBytes();
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER);
            buf.writeNullTerminatedString(pluginName);
            buf.writeFixedLengthBytes(authData);
        }

        public String toString() {
            return String.format(
                    "AuthNextFactor(Request)[pluginName = %s, authDataLength = %d]",
                    pluginName, authData != null ? authData.length : -1
            );
        }
    }

    /**
     * Created on 2024/5/24.
     * quit: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_quit.html
     * statistics: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_statistics.html
     * debug: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_debug.html
     * ping: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_ping.html
     */
    public static class SimpleCommand extends RequestPacket {

        int command;

        @Override
        public int getCommand() {
            return command;
        }

        @Override
        public boolean isQuit() {
            return command == ServerCommands.QUIT;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            command = buf.readInt1();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(command);
        }

        @Override
        public String toString() {
            return String.format(
                    "SimpleCommand[command = %d]", command
            );
        }
    }

    /**
     * Created on 2024/5/21.
     */
    public static class Query extends RequestPacket {

        static final int COMMAND = ServerCommands.QUERY;

        String sql;

        @Override
        public int getCommand() {
            return COMMAND;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(COMMAND, buf.readInt1());
            if (context.hasCapability(CapabilitiesFlags.QUERY_ATTRIBUTES)) {
                long parameterCount = buf.readLengthEncodedInteger();
                buf.assertEquals(1, buf.readLengthEncodedInteger());
                if (parameterCount > 0) {
                    buf.skipFixedLength((parameterCount + 7) / 8);
                    buf.assertEquals(1, buf.readInt1());
                    for (int index = 0; index < parameterCount; index++) {
                        //
                    }
                }
            }
            sql = buf.readRestOfPacketString();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(COMMAND);
            buf.writeFixedLengthString(sql);
        }

        @Override
        public String toString() {
            return String.format(
                    "Query[sql = %s]", sql
            );
        }
    }

    /**
     * Created on 2024/5/21.
     */
    public static class StatementPrepare extends RequestPacket {

        static final int COMMAND = ServerCommands.STATEMENT_PREPARE;

        String sql;

        @Override
        public int getCommand() {
            return COMMAND;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(COMMAND, buf.readInt1());
            sql = buf.readRestOfPacketString();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(COMMAND);
            buf.writeFixedLengthString(sql);
        }
    }

    /**
     * Created on 2024/5/21.
     *
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
     */
    public static class StatementExecute extends RequestPacket {

        static final int COMMAND = ServerCommands.STATEMENT_EXECUTE;

        static final int CURSOR_TYPE_NO_CURSOR = 0;
        static final int CURSOR_TYPE_READ_ONLY = 1;
        static final int CURSOR_TYPE_FOR_UPDATE = 2;
        static final int CURSOR_TYPE_SCROLLABLE = 3;

        int statementId;

        int cursorType;

        @Override
        public int getCommand() {
            return COMMAND;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(COMMAND, buf.readInt1());
            statementId = buf.readInt4();
            cursorType = buf.readInt1();
            buf.assertEquals(1, buf.readInt4()); // iteration count
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(COMMAND);
            buf.writeInt4(statementId);
            buf.writeInt1(cursorType);
            buf.writeInt4(1); // iteration count
        }
    }

    /**
     * Created on 2024/5/24.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
     */
    public static class StatementClose extends RequestPacket {

        static final int COMMAND = ServerCommands.STATEMENT_CLOSE;

        int statementId;

        @Override
        public int getCommand() {
            return COMMAND;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(COMMAND, buf.readInt1());
            statementId = buf.readInt4();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(COMMAND);
            buf.writeInt4(statementId);
        }
    }

    /**
     * Created on 2024/5/24.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html
     */
    public static class StatementReset extends RequestPacket {

        static final int COMMAND = ServerCommands.STATEMENT_RESET;

        int statementId;

        @Override
        public int getCommand() {
            return COMMAND;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(COMMAND, buf.readInt1());
            statementId = buf.readInt4();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(COMMAND);
            buf.writeInt4(statementId);
        }
    }
}
