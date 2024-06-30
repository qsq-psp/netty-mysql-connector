package indi.qsq.mysql;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.Serializable;
import java.net.SocketAddress;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;

/**
 * Created on 2024/5/17.
 * Maintain protocol states
 */
public class MysqlConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlConnection.class);

    static class AfterConfig implements Cloneable, Serializable {

        private static final long serialVersionUID = 0x592D239BB348A508L;

        /**
         * {@link CapabilitiesFlags}
         */
        int capabilitiesFlags;

        /**
         * {@link PreferenceFlags}
         */
        int preferenceFlags;

        int maxPacketSize = 0xffffff;

        /**
         * If SSL is not used, this field is null
         */
        SslContext sslContext;

        @Nullable
        RSAPublicKey publicKey;

        /**
         * After start, this field is not null
         */
        String userName;

        /**
         * After start, this field is not null
         */
        String password;

        @Nullable
        String defaultSchema;

        int zstdCompressionLevel;

        int quitTimeout = 3000;

        public AfterConfig() {
            super();
        }

        public AfterConfig(@NotNull AfterConfig that) {
            super();
            this.capabilitiesFlags = that.capabilitiesFlags;
            this.preferenceFlags = that.preferenceFlags;
            this.maxPacketSize = that.maxPacketSize;
            this.sslContext = that.sslContext;
            this.publicKey = that.publicKey;
            this.userName = that.userName;
            this.password = that.password;
            this.defaultSchema = that.defaultSchema;
            this.zstdCompressionLevel = that.zstdCompressionLevel;
            this.quitTimeout = that.quitTimeout;
        }

        public void enableSSL() throws SSLException {
            sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        }

//        public void enableDeflateCompression() {
//            capabilitiesFlags |= CapabilitiesFlags.COMPRESS;
//        }
//
//        /**
//         * If both CLIENT_COMPRESS and CLIENT_ZSTD_COMPRESSION_ALGORITHM are set then zlib is used.
//         * @param level min = 0, max = 22
//         */
//        public void enableZstdCompression(int level) {
//            if (0 <= level && level < 0x80) {
//                capabilitiesFlags &= ~CapabilitiesFlags.COMPRESS;
//                capabilitiesFlags |= CapabilitiesFlags.ZSTD_COMPRESSION_ALGORITHM;
//                zstdCompressionLevel = level;
//            } else {
//                capabilitiesFlags &= ~CapabilitiesFlags.ZSTD_COMPRESSION_ALGORITHM;
//            }
//        }

        public void prepare() {
            capabilitiesFlags |= CapabilitiesFlags.LONG_PASSWORD
                    | CapabilitiesFlags.FOUND_ROWS
                    | CapabilitiesFlags.LONG_FLAG
                    | CapabilitiesFlags.PROTOCOL_41
                    | CapabilitiesFlags.TRANSACTIONS
                    | CapabilitiesFlags.PLUGIN_AUTH
                    | CapabilitiesFlags.CONNECT_ATTRS
                    | CapabilitiesFlags.PLUGIN_AUTH_LENENC_CLIENT_DATA
                    | CapabilitiesFlags.DEPRECATE_EOF;
            capabilitiesFlags &= ~(
                    CapabilitiesFlags.MULTI_STATEMENTS
                    | CapabilitiesFlags.MULTI_RESULTS
                    | CapabilitiesFlags.PS_MULTI_RESULTS
            );
            if (sslContext != null) {
                capabilitiesFlags |= CapabilitiesFlags.SSL;
            }
            if (defaultSchema != null) {
                capabilitiesFlags |= CapabilitiesFlags.CONNECT_WITH_DB;
            }
            preferenceFlags |= PreferenceFlags.ALLOW_PUBLIC_KEY_RETRIEVAL;
            if (userName == null || userName.isEmpty()) {
                throw new MysqlException();
            }
            if (password == null) {
                password = "";
            }
            if (defaultSchema != null && defaultSchema.isEmpty()) {
                defaultSchema = null;
            }
        }
    }

    static class BeforeConfig extends AfterConfig {

        private static final long serialVersionUID = 0x1923DA859F4ABD88L;

        static final String ADDRESS = "127.0.0.1";

        String address = ADDRESS;

        static final int PORT = 3306;

        int port = PORT;

        int connectTimeout;
    }

    @NotNull
    public static Future<MysqlConnection> create(@NotNull EventLoopGroup group, @NotNull BeforeConfig config) {
        final Promise<MysqlConnection> promise = group.next().newPromise();
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new ChannelInboundHandlerAdapter()); // dummy
        bootstrap.option(ChannelOption.AUTO_READ, false);
        if (config.connectTimeout > 0) {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout);
        }
        final ChannelFuture channelFuture = bootstrap.connect(config.address, config.port);
        channelFuture.addListener(future1 -> {
            if (future1.isSuccess()) {
                MysqlConnection mysqlConnection = new MysqlConnection(new AfterConfig(config), channelFuture.channel());
                mysqlConnection.openFuture().addListener(future2 -> {
                    if (future2.isSuccess()) {
                        promise.setSuccess(mysqlConnection);
                    } else {
                        promise.setFailure(future2.cause());
                    }
                });
            } else {
                promise.setFailure(future1.cause());
            }
        });
        return promise;
    }

    @NotNull
    final AfterConfig config;

    @NotNull
    final Channel channel;

    final ChannelPromise openPromise;

    /**
     * {@link ProtocolStates}
     * Only write this state in channel.executor() thread, no synchronization needed
     */
    private int state;

    int[] serverVersion;

    ResultHeader resultHeader;

    @NotNull
    final HashMap<Integer, PreparedStatement> preparedStatementMap = new HashMap<>();

    MysqlConnection(@NotNull AfterConfig config, @NotNull Channel channel) {
        super();
        config.prepare();
        this.config = config;
        this.channel = channel;
        this.openPromise = channel.newPromise();
        this.state = ProtocolStates.SYN;
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(HandlerNames.CODEC, new Codec());
        channel.read();
    }

    public int getState() {
        return state;
    }

    private void setState(int nextState) throws MysqlException {
        if (hasPreference(PreferenceFlags.ACCESS_THREAD_CHECK) && !channel.eventLoop().inEventLoop()) {
            LOGGER.error("Setting protocol state from {} to {} in thread {}", state, nextState, Thread.currentThread().getName());
            throw new MysqlException("Setting protocol state outside event loop");
        }
        LOGGER.trace("Setting protocol state from {} to {}", state, nextState);
        state = nextState;
    }

    /**
     * @param flag {@link CapabilitiesFlags}
     * @return true if flag is set
     */
    public boolean hasCapability(int flag) {
        return (config.capabilitiesFlags & flag) != 0;
    }

    /**
     * @param flag {@link PreferenceFlags}
     * @return true if flag is set
     */
    public boolean hasPreference(int flag) {
        return (config.preferenceFlags & flag) != 0;
    }

    @NotNull
    @SuppressWarnings("UnusedReturnValue")
    public ChannelFuture writeAndFlush(@NotNull RequestPacket packet, @NotNull Promise<?> promise) {
        return channel.writeAndFlush(packet).addListener(future -> {
            if (!future.isSuccess()) {
                promise.setFailure(future.cause());
            }
        });
    }

    @NotNull
    public Future<ResponsePacket.OK> ping() {
        final Promise<ResponsePacket.OK> promise = channel.eventLoop().newPromise();
        final RequestPacket.SimpleCommand packet = new RequestPacket.SimpleCommand();
        packet.command = ServerCommands.PING;
        packet.responseHandler = new PacketCollector.OkCollector(promise);
        writeAndFlush(packet, promise);
        return promise;
    }

    @NotNull
    public Future<ResponsePacket.EOF> execute(@NotNull String sql) {
        final Promise<ResponsePacket.EOF> promise = channel.eventLoop().newPromise();
        final RequestPacket.Query packet = new RequestPacket.Query();
        packet.sql = sql;
        packet.responseHandler = new PacketCollector.EndCollector(promise);
        writeAndFlush(packet, promise);
        return promise;
    }

    @NotNull
    public Future<ResultSet> query(@NotNull String sql) {
        final Promise<ResultSet> promise = channel.eventLoop().newPromise();
        final RequestPacket.Query packet = new RequestPacket.Query();
        packet.sql = sql;
        packet.responseHandler = new PacketCollector.ResultSetCollector(promise);
        writeAndFlush(packet, promise);
        return promise;
    }

    public void query(@Nullable PacketCollector.ResultHeaderConsumer headerConsumer, @NotNull String sql) {
        final RequestPacket.Query packet = new RequestPacket.Query();
        packet.sql = sql;
        packet.responseHandler = new PacketCollector.ResultConsumerCollector(headerConsumer);
        final ChannelFuture future1 = channel.writeAndFlush(packet);
        if (headerConsumer != null) {
            future1.addListener(future2 -> {
                if (!future1.isSuccess()) {
                    headerConsumer.error(null);
                }
            });
        }
    }

    @NotNull
    public Future<ResponsePacket.EOF> prepareAndExecute(@NotNull String sql, Object... parameterValues) {
        return channel.eventLoop().newFailedFuture(new MysqlException());
    }

    @NotNull
    public Future<ResponsePacket.EOF> prepareAndQuery(@NotNull String sql, Object... parameterValues) {
        return channel.eventLoop().newFailedFuture(new MysqlException());
    }

    @NotNull
    public Future<ResponsePacket.EOF> prepareAndQuery(@Nullable PacketCollector.ResultHeaderConsumer headerConsumer, @NotNull String sql, Object... parameterValues) {
        return channel.eventLoop().newFailedFuture(new MysqlException());
    }

    @NotNull
    public Future<PreparedStatement> prepare(String sql) {
        final Promise<PreparedStatement> promise1 = channel.eventLoop().newPromise();
        final Promise<ResultHeader.StatementPrepareOK> promise2 = channel.eventLoop().newPromise();
        promise2.addListener(future -> {
            if (promise2.isSuccess()) {
                PreparedStatement preparedStatement = new PreparedStatement(this, promise2.get());
                preparedStatementMap.put(preparedStatement.resolved.statementId, preparedStatement);
                promise1.setSuccess(preparedStatement);
            } else {
                promise1.setFailure(promise2.cause());
            }
        });
        final RequestPacket.StatementPrepare packet = new RequestPacket.StatementPrepare();
        packet.sql = sql;
        packet.responseHandler = new PacketCollector.StatementPrepareCollector(promise2);
        channel.writeAndFlush(packet);
        return promise1;
    }

    @NotNull
    public ChannelFuture openFuture() {
        return openPromise;
    }

    protected ChannelFuture shutdownOutput() {
        return ((NioSocketChannel) channel).shutdownOutput();
    }

    public ChannelFuture close() {
        return channel.close();
    }

    public ChannelFuture closeFuture() {
        return channel.closeFuture();
    }

    /**
     * Created on 2024/6/16.
     */
    class Codec extends ByteToMessageDecoder implements ChannelOutboundHandler {

        private final ArrayDeque<RequestPacket> packetQueue = new ArrayDeque<>();

        /**
         * The sequence-id is incremented with each packet and may wrap around.
         * It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
         */
        private int sequence;

        /**
         * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html
         *
         * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
         * {@link ByteBuf} has nothing to read when return from this method or till nothing was read from the input
         * {@link ByteBuf}.
         *
         * @param context       the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
         * @param in            the {@link ByteBuf} from which to read data
         * @param out           the {@link List} to which decoded messages should be added
         */
        @Override
        protected void decode(@NotNull ChannelHandlerContext context, @NotNull ByteBuf in, @NotNull List<Object> out) {
            while (true) {
                int readable = in.readableBytes();
                if (readable < 4) {
                    break;
                }
                int header = in.readIntLE();
                int length = 0xffffff & header;
                if (readable < 4 + length) {
                    in.readerIndex(in.readerIndex() - 4);
                    break;
                }
                sequence = 0xff & (header >> 24);
                ByteBuf payload = context.alloc().heapBuffer(length);
                try {
                    in.readBytes(payload, length);
                    decode(context, new MysqlByteBuf(payload), out);
                } catch (MysqlException e) {
                    e.printStackTrace();
                } finally {
                    payload.release();
                }
            }
        }

        private void decode(@NotNull ChannelHandlerContext context, @NotNull MysqlByteBuf buf, @NotNull List<Object> out) throws MysqlException {
            ResponsePacket packet = null;
            switch (getState()) {
                case ProtocolStates.SYN: {
                    int version = buf.getInt1();
                    if (version == ResponsePacket.HandshakeV10.PROTOCOL_VERSION) {
                        packet = new ResponsePacket.HandshakeV10();
                    } else if (version == ResponsePacket.HandshakeV9.PROTOCOL_VERSION) {
                        packet = new ResponsePacket.HandshakeV9();
                    }
                    out.add(packet);
                    break;
                }
                case ProtocolStates.AUTH: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.OK.PACKET_HEADER) {
                        packet = new ResponsePacket.OK();
                        if (context.pipeline().get(HandlerNames.AUTH) != null) {
                            throw new MysqlException("Auth handler not self removed on finishing auth");
                        }
                        idle(context);
                        openPromise.setSuccess();
                    } else if (type == ResponsePacket.AuthMoreData.PACKET_HEADER) {
                        packet = new ResponsePacket.AuthMoreData();
                    } else if (type == ResponsePacket.AuthNextFactor.PACKET_HEADER) {
                        packet = new ResponsePacket.AuthNextFactor();
                    } else if (type == ResponsePacket.AuthSwitchRequest.PACKET_HEADER) {
                        ChannelHandler handler = context.pipeline().get(HandlerNames.AUTH);
                        if (handler != null) {
                            LOGGER.info("Auth switch removing {}", handler);
                            context.pipeline().remove(handler);
                        }
                        packet = new ResponsePacket.AuthSwitchRequest();
                    } else {
                        packet = new ResponsePacket.Error();
                    }
                    out.add(packet);
                    break;
                }
                case ProtocolStates.PING:
                case ProtocolStates.RESET_CONNECTION:
                    packet = new ResponsePacket.OK();
                    out.add(packet);
                    idle(context);
                    break;
                case ProtocolStates.HANDSHAKE:
                case ProtocolStates.QUIT:
                    packet = new ResponsePacket.Error();
                    out.add(packet);
                    break;
                case ProtocolStates.INIT_DB:
                case ProtocolStates.DEBUG:
                case ProtocolStates.STATEMENT_RESET:
                case ProtocolStates.SET_OPTION:
                    if (buf.getInt1() == ResponsePacket.OK.PACKET_HEADER) {
                        packet = new ResponsePacket.OK();
                    } else {
                        packet = new ResponsePacket.Error();
                    }
                    out.add(packet);
                    idle(context);
                    break;
                case ProtocolStates.STATEMENT_PREPARE:
                    if (buf.getInt1() == ResultHeader.StatementPrepareOK.PACKET_HEADER) {
                        packet = resultHeader = new ResultHeader.StatementPrepareOK();
                        setState(ProtocolStates.STATEMENT_PREPARE_COLUMNS);
                    } else {
                        packet = new ResponsePacket.Error();
                    }
                    out.add(packet);
                    break;
                case ProtocolStates.STATEMENT_PREPARE_COLUMNS:
                    if (resultHeader.hasNext()) {
                        if (!hasCapability(CapabilitiesFlags.DEPRECATE_EOF) && buf.getInt1() == ResponsePacket.EOF.PACKET_HEADER) {
                            packet = new ResponsePacket.EOF(true);
                        } else {
                            packet = resultHeader.next(MysqlConnection.this);
                        }
                        if (hasCapability(CapabilitiesFlags.DEPRECATE_EOF) && !resultHeader.hasNext()) {
                            resultHeader.finish();
                            idle(context);
                        }
                    } else {
                        resultHeader.finish();
                        if (!hasCapability(CapabilitiesFlags.DEPRECATE_EOF) && buf.getInt1() == ResponsePacket.EOF.PACKET_HEADER) {
                            packet = new ResponsePacket.EOF(false);
                            idle(context);
                        } else {
                            throw new MysqlException();
                        }
                    }
                    out.add(packet);
                    if (resultHeader.pipeline()) {
                        out.add(resultHeader);
                    }
                    break;
                // The query response packet is a meta packet which can be one of: ERR_Packet, OK_Packet, LOCAL INFILE Request (not supported), Text Resultset
                case ProtocolStates.QUERY: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else if (type == ResponsePacket.OK.PACKET_HEADER) {
                        packet = new ResponsePacket.OK();
                        idle(context);
                    } else {
                        packet = resultHeader = new ResultHeader.ResultTextHeader();
                        setState(ProtocolStates.QUERY_COLUMNS);
                    }
                    out.add(packet);
                    break;
                }
                case ProtocolStates.QUERY_COLUMNS: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else if (resultHeader.hasNext()) {
                        packet = resultHeader.next(MysqlConnection.this);
                    } else if (type == ResponsePacket.EOF.PACKET_HEADER) {
                        resultHeader.finish();
                        if (hasCapability(CapabilitiesFlags.DEPRECATE_EOF)) {
                            packet = new ResponsePacket.OK();
                            idle(context);
                        } else {
                            packet = new ResponsePacket.EOF(true);
                            setState(ProtocolStates.QUERY_ROWS);
                        }
                    } else {
                        resultHeader.finish();
                        if (resultHeader.pipeline()) {
                            out.add(resultHeader);
                        }
                        if (hasPreference(PreferenceFlags.LAZY_DECODE_TEXT)) {
                            packet = new ResultRow.RawText();
                        } else {
                            packet = new ResultRow.DecodedText();
                        }
                        setState(ProtocolStates.QUERY_ROWS);
                    }
                    out.add(packet);
                    if (resultHeader.pipeline()) {
                        out.add(resultHeader);
                    }
                    break;
                }
                case ProtocolStates.QUERY_ROWS: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else if (type == ResponsePacket.EOF.PACKET_HEADER) {
                        if (hasCapability(CapabilitiesFlags.DEPRECATE_EOF)) {
                            packet = new ResponsePacket.OK();
                        } else {
                            packet = new ResponsePacket.EOF(false);
                        }
                        idle(context);
                    } else if (hasPreference(PreferenceFlags.LAZY_DECODE_TEXT)) {
                        packet = new ResultRow.RawText();
                    } else {
                        packet = new ResultRow.DecodedText();
                    }
                    out.add(packet);
                    break;
                }
                // Similar to the COM_QUERY Response a COM_STMT_EXECUTE returns either: an OK_Packet, an ERR_Packet, Binary Protocol Resultset
                case ProtocolStates.STATEMENT_EXECUTE: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else if (type == ResponsePacket.OK.PACKET_HEADER) {
                        packet = new ResponsePacket.OK();
                        idle(context);
                    } else {
                        packet = resultHeader = new ResultHeader.ResultBinaryHeader();
                        setState(ProtocolStates.STATEMENT_EXECUTE_COLUMNS);
                    }
                    out.add(packet);
                    break;
                }
                case ProtocolStates.STATEMENT_EXECUTE_COLUMNS:
                    if (buf.getInt1() == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else {
                        packet = resultHeader.next(MysqlConnection.this);
                        if (!resultHeader.hasNext()) {
                            resultHeader.finish();
                            setState(ProtocolStates.STATEMENT_EXECUTE_ROWS);
                        }
                    }
                    out.add(packet);
                    break;
                case ProtocolStates.STATEMENT_EXECUTE_ROWS: {
                    int type = buf.getInt1();
                    if (type == ResponsePacket.Error.PACKET_HEADER) {
                        packet = new ResponsePacket.Error();
                        idle(context);
                    } else if (type == ResponsePacket.OK.PACKET_HEADER) {
                        if (!hasCapability(CapabilitiesFlags.DEPRECATE_EOF)) {
                            throw new MysqlException("EOF not deprecated statement execute");
                        }
                        packet = new ResponsePacket.OK();
                        idle(context);
                    } else if (type == ResponsePacket.EOF.PACKET_HEADER) {
                        if (hasCapability(CapabilitiesFlags.DEPRECATE_EOF)) {
                            throw new MysqlException("EOF deprecated statement execute");
                        }
                        packet = new ResponsePacket.EOF(false);
                        idle(context);
                    } else if (hasPreference(PreferenceFlags.LAZY_DECODE_BINARY)) {
                        packet = new ResultRow.RawBinary();
                    } else {
                        packet = new ResultRow.DecodedBinary();
                    }
                    out.add(packet);
                    break;
                }
            }
            if (packet != null) {
                packet.read(buf, MysqlConnection.this);
                LOGGER.info("decode {} {}", packet, sequence);
            } else {
                LOGGER.info("decode null {}", ByteBufUtil.prettyHexDump(buf.content()));
                throw new MysqlException("Fail to decode packet at state " + getState());
            }
            if (packet instanceof ResponsePacket.HandshakeV10) {
                handshake(context, (ResponsePacket.HandshakeV10) packet);
            }
            if (packet instanceof AuthPlugin.NameHolder) {
                auth(context, ((AuthPlugin.NameHolder) packet).getAuthPluginName());
            }
        }

        private void handshake(@NotNull ChannelHandlerContext context, @NotNull ResponsePacket.HandshakeV10 handshakeRequest) {
            serverVersion = handshakeRequest.parseVersion();
            config.capabilitiesFlags &= handshakeRequest.serverCapabilitiesFlags;
            if (hasCapability(CapabilitiesFlags.SSL)) {
                LOGGER.info("handshake SSL");
                setState(ProtocolStates.SSL);
                RequestPacket.SslRequest sslRequest = new RequestPacket.SslRequest();
                sslRequest.clientCapabilitiesFlags = config.capabilitiesFlags;
                sslRequest.maxPacketSize = config.maxPacketSize;
                sslRequest.characterSet = CharacterSets.UTF8_GENERAL_CI;
                sequence++;
                writeAndFlush(context, sslRequest).addListener(future -> {
                    if (future.isSuccess()) {
                        context.pipeline().addBefore(HandlerNames.CODEC, HandlerNames.SSL, config.sslContext.newHandler(context.alloc()));
                    } else {
                        LOGGER.error("handshake {} {}", handshakeRequest, sslRequest, future.cause());
                    }
                });
            } else {
                LOGGER.info("handshake");
                setState(ProtocolStates.HANDSHAKE);
            }
        }

        private void auth(@NotNull ChannelHandlerContext context, String pluginName) {
            context.pipeline().addAfter(context.name(), HandlerNames.AUTH, AuthPlugin.forName(MysqlConnection.this, pluginName));
        }

        private void idle(@NotNull ChannelHandlerContext context) {
            setState(ProtocolStates.IDLE);
            context.executor().execute(() -> flush(context));
        }

        @Override
        public void channelReadComplete(@NotNull ChannelHandlerContext context) {
            context.read();
        }

        @Override
        public void channelInactive(@NotNull ChannelHandlerContext context) {
            openPromise.tryFailure(new MysqlException("Channel inactive"));
            context.fireChannelInactive();
        }

        @Override
        public void userEventTriggered(@NotNull ChannelHandlerContext context, Object event) {
            if (event instanceof SslHandshakeCompletionEvent) {
                Throwable cause = ((SslHandshakeCompletionEvent) event).cause();
                if (cause != null) {
                    LOGGER.error("userEventTriggered", cause);
                    context.close();
                } else {
                    setState(ProtocolStates.HANDSHAKE);
                    flush(context);
                }
            }
            context.fireUserEventTriggered(event);
        }

        @Override
        public void bind(@NotNull ChannelHandlerContext context, SocketAddress localAddress, @NotNull ChannelPromise promise) {
            context.bind(localAddress, promise); // default NOP implementation in adapter
        }

        @Override
        public void connect(@NotNull ChannelHandlerContext context, SocketAddress remoteAddress, SocketAddress localAddress, @NotNull ChannelPromise promise) {
            context.connect(remoteAddress, localAddress, promise); // default NOP implementation in adapter
        }

        @Override
        public void disconnect(@NotNull ChannelHandlerContext context, @NotNull ChannelPromise promise) {
            context.disconnect(promise); // default NOP implementation in adapter
        }

        @Override
        public void close(@NotNull ChannelHandlerContext context, @NotNull ChannelPromise promise) {
            context.close(promise);
        }

        @Override
        public void deregister(@NotNull ChannelHandlerContext context, @NotNull ChannelPromise promise) {
            context.deregister(promise); // default NOP implementation in adapter
        }

        @Override
        public void read(@NotNull ChannelHandlerContext context) {
            context.read(); // default NOP implementation in adapter
        }

        @Override
        public void write(@NotNull ChannelHandlerContext context, Object message, @NotNull ChannelPromise promise) {
            LOGGER.info("write {}", message);
            if (message instanceof RequestPacket) {
                RequestPacket packet = (RequestPacket) message;
                packet.writePromise = promise;
                packetQueue.addLast(packet);
                flush(context);
            } else {
                context.write(message, promise);
            }
        }

        @Override
        public void flush(@NotNull ChannelHandlerContext context) {
            if (getState() >= 0) {
                return;
            }
            while (!packetQueue.isEmpty()) {
                RequestPacket packet = packetQueue.removeFirst();
                packet = normalize(packet);
                if (packet != null) {
                    int command = packet.getCommand();
                    if (command >= 0) {
                        sequence = 0;
                    } else {
                        sequence++;
                    }
                    if (packet.responseHandler != null) {
                        context.pipeline().addLast(HandlerNames.COLLECTOR, packet.responseHandler);
                    }
                    writeAndFlush(context, packet);
                    setState(command);
                    break;
                }
            }
        }

        private RequestPacket normalize(RequestPacket packet) {
            if (packet instanceof RequestPacket.AuthNextFactor) {
                RequestPacket.AuthNextFactor authNextFactor = (RequestPacket.AuthNextFactor) packet;
                LOGGER.info("normalize {} {}", packet, getState());
                if (getState() == ProtocolStates.HANDSHAKE) {
                    RequestPacket.HandshakeResponse handshakeResponse = new RequestPacket.HandshakeResponse();
                    handshakeResponse.writePromise = authNextFactor.writePromise;
                    handshakeResponse.responseHandler = authNextFactor.responseHandler;
                    handshakeResponse.clientCapabilitiesFlags = config.capabilitiesFlags;
                    handshakeResponse.maxPacketSize = config.maxPacketSize;
                    handshakeResponse.characterSet = CharacterSets.UTF8_GENERAL_CI;
                    handshakeResponse.userName = config.userName;
                    handshakeResponse.schemaName = config.defaultSchema;
                    handshakeResponse.zstdCompressionLevel = config.zstdCompressionLevel;
                    handshakeResponse.pluginName = authNextFactor.pluginName;
                    handshakeResponse.authResponse = authNextFactor.authData;
                    handshakeResponse.clientAttributes = new HashMap<>();
                    packet = handshakeResponse;
                } else {
                    RequestPacket.AuthSwitchResponse authSwitchResponse = new RequestPacket.AuthSwitchResponse();
                    authSwitchResponse.writePromise = authNextFactor.writePromise;
                    authSwitchResponse.responseHandler = authNextFactor.responseHandler;
                    authSwitchResponse.authResponse = authNextFactor.authData;
                    packet = authSwitchResponse;
                }
            }
            return packet;
        }

        private ChannelPromise writeAndFlush(@NotNull ChannelHandlerContext context, @NotNull RequestPacket packet) {
            if (packet.writePromise == null) {
                packet.writePromise = context.newPromise();
            }
            try {
                context.writeAndFlush(encode(context, packet), packet.writePromise);
            } catch (Exception e) {
                LOGGER.error("writeAndFlush", e);
                packet.writePromise.setFailure(e);
            }
            return packet.writePromise;
        }

        @NotNull
        private ByteBuf encode(@NotNull ChannelHandlerContext context, @NotNull RequestPacket packet) throws MysqlException {
            LOGGER.info("encode {}", packet);
            final ByteBuf data = context.alloc().buffer();
            try {
                data.writerIndex(4);
                packet.write(new MysqlByteBuf(data), MysqlConnection.this);
                data.setIntLE(0, data.writerIndex() - 4);
                data.setByte(3, sequence);
                return data.retain();
            } finally {
                data.release();
            }
        }
    }
}
