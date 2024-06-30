package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Created on 2024/5/19.
 */
public abstract class ResponsePacket extends Packet {

    public boolean isSegment() {
        return false;
    }

    /**
     * Created on 2024/5/18.
     *
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake.html
     */
    public static abstract class AbstractHandshake extends ResponsePacket implements AuthPlugin.DataHolder {

        String serverVersion;

        int threadId;

        byte[] scramble;

        @NotNull
        public int[] parseVersion() {
            final String[] segments = serverVersion.split("\\.");
            final int length = segments.length;
            final int[] values = new int[length];
            for (int index = 0; index < length; index++) {
                values[index] = Integer.parseInt(segments[index]);
            }
            return values;
        }

        @Override
        public byte[] getAuthData() {
            return scramble;
        }
    }

    /**
     * Created on 2024/5/18.
     *
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v9.html
     * deprecated since 3.21.0
     */
    public static class HandshakeV9 extends AbstractHandshake {

        static final int PROTOCOL_VERSION = 9;

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PROTOCOL_VERSION, buf.readInt1());
            serverVersion = buf.readNullTerminatedString();
            threadId = buf.readInt4();
            scramble = buf.readNullTerminatedBytes();
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PROTOCOL_VERSION);
            buf.writeNullTerminatedString(serverVersion);
            buf.writeInt4(threadId);
            buf.writeNullTerminatedBytes(scramble);
        }

        @Override
        public String toString() {
            return String.format(
                    "HandshakeV9[serverVersion = %s, threadId = %d, scrambleLength = %d]",
                    serverVersion, threadId, scramble != null ? scramble.length : -1
            );
        }
    }

    /**
     * Created on 2024/5/18.
     *
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
     */
    public static class HandshakeV10 extends AbstractHandshake implements AuthPlugin.NameHolder {

        static final int PROTOCOL_VERSION = 10;

        static final byte[] RESERVED_ZEROS = new byte[10];

        /**
         * {@link CapabilitiesFlags}
         */
        int serverCapabilitiesFlags; // 0xc7ffffff

        /**
         * {@link CharacterSets}
         */
        int characterSet; // 0x21, CharacterSets.UTF8_GENERAL_CI

        int statusFlags; // 0x0002

        String authPluginName; // caching_sha2_password

        public boolean hasCapability(int flag) {
            return (serverCapabilitiesFlags & flag) != 0;
        }

        @Override
        public String getAuthPluginName() {
            return authPluginName;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PROTOCOL_VERSION, buf.readInt1());
            serverVersion = buf.readNullTerminatedString();
            threadId = buf.readInt4();
            scramble = buf.readFixedLengthBytes(8);
            buf.assertEquals(0, buf.readInt1());
            serverCapabilitiesFlags = buf.readInt2();
            characterSet = buf.readInt1();
            statusFlags = buf.readInt2();
            serverCapabilitiesFlags |= buf.readInt2() << 16;
            final int authPluginDataLength = buf.readInt1();
            buf.assertEquals(RESERVED_ZEROS, buf.readFixedLengthBytes(RESERVED_ZEROS.length));
            scramble = AuthPlugin.concatBytes(scramble, buf.readFixedLengthBytes(Math.max(13, authPluginDataLength - 8)));
            if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH)) {
                authPluginName = buf.readNullTerminatedString();
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PROTOCOL_VERSION);
            buf.writeNullTerminatedString(serverVersion);
            buf.writeInt4(threadId);
            buf.writeFixedLengthBytes(Arrays.copyOf(scramble, 8));
            buf.writeInt1(0);
            buf.writeInt2(serverCapabilitiesFlags);
            buf.writeInt1(characterSet);
            buf.writeInt2(statusFlags);
            buf.writeInt2(serverCapabilitiesFlags >> 16);
            buf.writeInt1(Math.max(0, scramble.length - 8));
            buf.writeFixedLengthBytes(RESERVED_ZEROS);
            buf.writeFixedLengthBytes(AuthPlugin.sliceBytes(scramble, 8, 13));
            if (hasCapability(CapabilitiesFlags.PLUGIN_AUTH)) {
                buf.writeNullTerminatedString(authPluginName);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "HandshakeV10[serverVersion = %s, threadId = %d, capabilitiesFlags = 0x%x, characterSet = %d, statusFlags = 0x%x, authPluginName = %s, scrambleLength = %d]",
                    serverVersion, threadId, serverCapabilitiesFlags, characterSet, statusFlags, authPluginName, scramble != null ? scramble.length : -1
            );
        }
    }

    /**
     * Created on 2024/5/18.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
     * deprecated since 5.7.5
     */
    public static class EOF extends ResponsePacket {

        static final int PACKET_HEADER = 0xfe;

        final boolean segment;

        int warningCount;

        int statusFlags;

        public EOF(boolean segment) {
            super();
            this.segment = segment;
        }

        public EOF() {
            this(false);
        }

        @Override
        public boolean isSegment() {
            return segment;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PACKET_HEADER, buf.readInt1());
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                warningCount = buf.readInt2();
                statusFlags = buf.readInt2();
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER);
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.writeInt2(warningCount);
                buf.writeInt2(statusFlags);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "EOF[warningCount = %d, statusFlags = 0x%x]",
                    warningCount, statusFlags
            );
        }
    }

    /**
     * Created on 2024/5/18.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
     */
    public static class OK extends EOF {

        static final int PACKET_HEADER = 0x00;

        long affectedRows;

        long lastInsertedId;

        String statusInfo;

        byte[] sessionStateInfo;

        public OK() {
            super();
        }

        public boolean hasStatus(int flag) {
            return (statusFlags & flag) != 0;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertPredicate(value -> value == PACKET_HEADER || value == EOF.PACKET_HEADER, buf.readInt1()); // packet header
            affectedRows = buf.readLengthEncodedInteger();
            lastInsertedId = buf.readLengthEncodedInteger();
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                statusFlags = buf.readInt2();
                warningCount = buf.readInt2();
            } else if (context.hasCapability(CapabilitiesFlags.TRANSACTIONS)) {
                statusFlags = buf.readInt2();
            }
            if (context.hasCapability(CapabilitiesFlags.SESSION_TRACK)) {
                statusInfo = buf.readLengthEncodedString();
                if (hasStatus(ServerStatus.SESSION_STATE_CHANGED)) {
                    sessionStateInfo = buf.readLengthEncodedBytes();
                }
            } else {
                statusInfo = buf.readRestOfPacketString();
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER); // packet header
            buf.writeLengthEncodedInteger(affectedRows);
            buf.writeLengthEncodedInteger(lastInsertedId);
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.writeInt2(statusFlags);
                buf.writeInt2(warningCount);
            } else if (context.hasCapability(CapabilitiesFlags.TRANSACTIONS)) {
                buf.writeInt2(statusFlags);
            }
            if (context.hasCapability(CapabilitiesFlags.SESSION_TRACK)) {
                buf.writeLengthEncodedString(statusInfo);
                if (hasStatus(ServerStatus.SESSION_STATE_CHANGED)) {
                    buf.writeLengthEncodedBytes(sessionStateInfo);
                }
            } else {
                buf.writeFixedLengthString(statusInfo);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "OK[affectedRows = %d, lastInsertedId = %d, statusFlags = 0x%x, warningCount = %d, statusInfo = %s]",
                    affectedRows, lastInsertedId, statusFlags, warningCount, statusInfo
            );
        }
    }

    /**
     * Created on 2024/5/18.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
     */
    public static class Error extends ResponsePacket {

        static final int PACKET_HEADER = 0xff;

        int errorCode;

        String sqlState;

        String errorMessage;

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PACKET_HEADER, buf.readInt1()); // packet header
            errorCode = buf.readInt2();
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.assertEquals('#', buf.readInt1()); // sql state marker
                sqlState = buf.readFixedLengthString(5);
            }
            errorMessage = buf.readRestOfPacketString();
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER);
            buf.writeInt2(errorCode);
            if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
                buf.writeInt1('#');
                buf.assertEquals(5, buf.writeFixedLengthString(sqlState));
            }
            buf.writeFixedLengthString(errorMessage);
        }

        @Override
        public String toString() {
            return String.format(
                    "Error[code = %d, message = %s, sqlState = %s]",
                    errorCode, errorMessage, sqlState
            );
        }
    }

    /**
     * Created on 2024/5/27.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
     * This is a response packet (from server to client)
     * Next: {@link RequestPacket.AuthSwitchResponse}; or closing the connection
     */
    public static class AuthSwitchRequest extends ResponsePacket implements AuthPlugin.NameHolder, AuthPlugin.DataHolder {

        static final int PACKET_HEADER = 0xfe;

        String pluginName;

        byte[] authData;

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
            // authData = buf.readRestOfPacketBytes();
            authData = buf.readNullTerminatedBytes();
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
                    "AuthSwitchRequest[pluginName = %s, authDataLength = %d]",
                    pluginName, authData != null ? authData.length : -1
            );
        }
    }

    /**
     * Created on 2024/5/27.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html
     */
    public static class AuthMoreData extends ResponsePacket implements AuthPlugin.DataHolder {

        static final int PACKET_HEADER = 0x01;

        byte[] authData;

        @Override
        public byte[] getAuthData() {
            return authData;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PACKET_HEADER, buf.readInt1());
            authData = buf.readRestOfPacketBytes();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER);
            buf.writeFixedLengthBytes(authData);
        }
    }

    /**
     * Created on 2024/6/1.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_next_factor_request.html
     * Next Authentication method Packet in Multi Factor Authentication
     * If both server and the client support MULTI_FACTOR_AUTHENTICATION capability, server can send this packet to ask client to initiate next authentication method in Multi Factor Authentication process.
     * Next: Protocol::AuthNextFactor: or closing the connection.
     */
    public static class AuthNextFactor extends ResponsePacket implements AuthPlugin.NameHolder, AuthPlugin.DataHolder {

        static final int PACKET_HEADER = 0x02;

        String pluginName;

        byte[] authData;

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
                    "AuthNextFactor(Response)[pluginName = %s, authDataLength = %d]",
                    pluginName, authData != null ? authData.length : -1
            );
        }
    }
}
