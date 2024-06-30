package indi.qsq.mysql;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 2024/5/22.
 */
public class PreparedStatement implements MysqlImmediateCloseable {

    /**
     * Created on 2024/6/16.
     */
    public static class Parameter extends PacketObject {

        final int index;

        /**
         * {@link FieldTypes}
         */
        int type;

        /**
         * only {@link ColumnFlags}.UNSIGNED
         */
        int flags;

        String name;

        Object value;

        public Parameter(int index) {
            super();
            this.index = index;
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(type);
            buf.writeInt1(flags);
            buf.writeLengthEncodedString(name);
            switch (type) {
                case FieldTypes.TINY:
                    buf.writeInt1(((Number) value).intValue());
                    break;
                case FieldTypes.SHORT:
                case FieldTypes.YEAR:
                    buf.writeInt2(((Number) value).intValue());
                    break;
                case FieldTypes.INT24:
                case FieldTypes.LONG:
                    buf.writeInt4(((Number) value).intValue());
                    break;
                case FieldTypes.LONG_LONG:
                    buf.writeInt8(((Number) value).longValue());
                    break;
                case FieldTypes.FLOAT:
                    buf.writeInt4(Float.floatToRawIntBits(((Number) value).floatValue()));
                    break;
                case FieldTypes.DOUBLE:
                    buf.writeInt8(Double.doubleToRawLongBits(((Number) value).doubleValue()));
                    break;
                case FieldTypes.VAR_CHAR:
                case FieldTypes.VAR_STRING:
                case FieldTypes.STRING:
                case FieldTypes.ENUM:
                case FieldTypes.SET:
                case FieldTypes.GEOMETRY:
                case FieldTypes.BIT:
                case FieldTypes.DECIMAL:
                case FieldTypes.NEW_DECIMAL:
                    buf.writeLengthEncodedString(value.toString());
                    break;
                case FieldTypes.TINY_BLOB:
                case FieldTypes.MEDIUM_BLOB:
                case FieldTypes.LONG_BLOB:
                case FieldTypes.BLOB:
                    if (value instanceof byte[]) {
                        buf.writeLengthEncodedBytes((byte[]) value);
                    } else {
                        buf.writeLengthEncodedBytes((ByteBuf) value);
                    }
                    break;
                case FieldTypes.DATE:
                case FieldTypes.DATE_TIME:
                case FieldTypes.TIMESTAMP:
                    ((MysqlDateTime) value).write(buf, context);
                    break;
                case FieldTypes.TIME:
                    ((MysqlTime) value).write(buf, context);
                    break;
            }
        }
    }

    @NotNull
    final MysqlConnection connection;

    @NotNull
    final ResultHeader.StatementPrepareOK resolved;

    @NotNull
    final Parameter[] parameters;

    final AtomicBoolean closed = new AtomicBoolean(false);

    public PreparedStatement(@NotNull MysqlConnection connection, @NotNull ResultHeader.StatementPrepareOK resolved) {
        super();
        this.connection = connection;
        this.resolved = resolved;
        this.parameters = new Parameter[resolved.parameters.length];
    }

    @Override
    public void close() throws MysqlException {
        if (closed.compareAndSet(false, true)) {
            RequestPacket.StatementClose request = new RequestPacket.StatementClose();
            request.statementId = resolved.statementId;
            connection.channel.writeAndFlush(request);
        }
    }

    @Override
    public String toString() {
        return "";
    }
}
