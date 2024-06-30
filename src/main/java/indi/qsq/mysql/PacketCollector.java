package indi.qsq.mysql;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2024/6/16.
 * Add consume control
 */
public abstract class PacketCollector extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PacketCollector.class);

    protected Promise<?> getPromise() {
        return null;
    }

    @Override
    public void handlerAdded(@NotNull ChannelHandlerContext context) {
        final Promise<?> promise = getPromise();
        if (promise != null) {
            promise.addListener(future -> {
                context.pipeline().remove(this);
                LOGGER.info("{} removed", this);
            });
        }
    }

    @Override
    public void handlerRemoved(@NotNull ChannelHandlerContext context) {
        final Promise<?> promise = getPromise();
        if (promise != null && !promise.isDone()) {
            promise.tryFailure(new MysqlException("Handler removed"));
        }
    }

    @Override
    public void channelInactive(@NotNull ChannelHandlerContext context) {
        final Promise<?> promise = getPromise();
        if (promise != null && !promise.isDone()) {
            promise.tryFailure(new MysqlException("Channel inactive"));
        }
        context.fireChannelInactive();
    }

    /**
     * Created on 2024/6/16.
     */
    public static class OkCollector extends PacketCollector {

        @NotNull
        final Promise<ResponsePacket.OK> promise;

        public OkCollector(@NotNull Promise<ResponsePacket.OK> promise) {
            super();
            this.promise = promise;
        }

        @Override
        @NotNull
        public Promise<ResponsePacket.OK> getPromise() {
            return promise;
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext context, Object message) {
            if (message instanceof ResponsePacket.OK) {
                promise.setSuccess((ResponsePacket.OK) message);
            } else if (message instanceof ResponsePacket.Error) {
                promise.setFailure(new MysqlException(message.toString()));
            }
            context.fireChannelRead(message);
        }

        @Override
        public String toString() {
            return String.format("OkCollector[id = %08x, promise = %s]", System.identityHashCode(this), promise);
        }
    }

    /**
     * Created on 2024/6/17.
     */
    public static class EndCollector extends PacketCollector {

        @NotNull
        final Promise<ResponsePacket.EOF> promise;

        public EndCollector(@NotNull Promise<ResponsePacket.EOF> promise) {
            super();
            this.promise = promise;
        }

        @Override
        @NotNull
        public Promise<ResponsePacket.EOF> getPromise() {
            return promise;
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext context, Object message) {
            if (message instanceof ResponsePacket.EOF) {
                ResponsePacket.EOF end = (ResponsePacket.EOF) message;
                if (!end.isSegment()) {
                    promise.setSuccess(end);
                }
            } else if (message instanceof ResponsePacket.Error) {
                promise.setFailure(new MysqlException(message.toString()));
            }
            context.fireChannelRead(message);
        }

        @Override
        public String toString() {
            return String.format("EndCollector[id = %08x, promise = %s]", System.identityHashCode(this), promise);
        }
    }

    /**
     * Created on 2024/6/29.
     */
    public static class StatementPrepareCollector extends PacketCollector {

        @NotNull
        final Promise<ResultHeader.StatementPrepareOK> promise;

        public StatementPrepareCollector(@NotNull Promise<ResultHeader.StatementPrepareOK> promise) {
            super();
            this.promise = promise;
        }

        @Override
        @NotNull
        public Promise<ResultHeader.StatementPrepareOK> getPromise() {
            return promise;
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext context, Object message) {
            if (message instanceof ResultHeader.StatementPrepareOK) {
                promise.setSuccess((ResultHeader.StatementPrepareOK) message);
            } else if (message instanceof ResponsePacket.Error) {
                promise.setFailure(new MysqlProtocolException((ResponsePacket.Error) message));
            } else {
                context.fireChannelRead(message);
            }
        }
    }

    /**
     * Created on 2024/6/16.
     */
    public static class ResultSetCollector extends PacketCollector {

        @NotNull
        final Promise<ResultSet> promise;

        ResultSet resultSet;

        public ResultSetCollector(@NotNull Promise<ResultSet> promise) {
            super();
            this.promise = promise;
        }

        @Override
        @NotNull
        public Promise<ResultSet> getPromise() {
            return promise;
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext context, Object message) {
            if (message instanceof ResponsePacket) {
                if (((ResponsePacket) message).isSegment()) {
                    return;
                }
                if (message instanceof ResultRow) {
                    resultSet.rows.add((ResultRow) message);
                } else if (message instanceof ResultHeader) {
                    resultSet = new ResultSet((ResultHeader) message);
                } else if (message instanceof ResponsePacket.Error) {
                    promise.setFailure(new MysqlException(((ResponsePacket.Error) message).errorMessage));
                } else if (message instanceof ResponsePacket.EOF) {
                    resultSet.end = (ResponsePacket.EOF) message;
                    promise.setSuccess(resultSet);
                }
            } else {
                context.fireChannelRead(message);
            }
        }

        @Override
        public String toString() {
            return String.format("ResultSetCollector[id = @%08x, promise = %s, resultSet = %s]",
                    System.identityHashCode(this), promise, resultSet);
        }
    }

    /**
     * Created on 2024/6/16.
     */
    public interface ResultHeaderConsumer {

        void error(@Nullable ResponsePacket.Error error);

        @Nullable
        ResultContentConsumer accept(@NotNull ResultHeader header);
    }

    /**
     * Created on 2024/6/16.
     */
    public interface ResultContentConsumer {

        void error(@Nullable ResponsePacket.Error error);

        void next(@NotNull ResultRow row);

        void finish(@NotNull ResponsePacket.EOF end);
    }

    public static class ResultConsumerCollector extends PacketCollector {

        ResultHeaderConsumer headerConsumer;

        ResultContentConsumer contentConsumer;

        public ResultConsumerCollector(ResultHeaderConsumer headerConsumer) {
            super();
            this.headerConsumer = headerConsumer;
        }

        @Override
        public void handlerRemoved(@NotNull ChannelHandlerContext context) {
            if (headerConsumer != null) {
                headerConsumer.error(null);
            } else if (contentConsumer != null) {
                contentConsumer.error(null);
            }
        }

        @Override
        public void channelInactive(@NotNull ChannelHandlerContext context) {
            handlerRemoved(context);
            context.fireChannelInactive();
        }

        @Override
        public void channelRead(@NotNull ChannelHandlerContext context, Object message) {
            if (message instanceof ResponsePacket) {
                if (((ResponsePacket) message).isSegment()) {
                    return;
                }
                if (message instanceof ResultRow) {
                    if (contentConsumer != null) {
                        contentConsumer.next((ResultRow) message);
                    }
                } else if (message instanceof ResultHeader) {
                    if (headerConsumer != null) {
                        contentConsumer = headerConsumer.accept((ResultHeader) message);
                        headerConsumer = null;
                    }
                } else if (message instanceof ResponsePacket.Error) {
                    if (headerConsumer != null) {
                        headerConsumer.error((ResponsePacket.Error) message);
                    } else if (contentConsumer != null) {
                        contentConsumer.error((ResponsePacket.Error) message);
                    }
                } else if (message instanceof ResponsePacket.EOF) {
                    if (contentConsumer != null) {
                        contentConsumer.finish((ResponsePacket.EOF) message);
                    }
                }
            } else {
                context.fireChannelRead(message);
            }
        }
    }
}
