package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Created on 2024/5/28.
 */
public abstract class ResultHeader extends ResponsePacket {

    protected ResultColumn[] columns;

    static final int SEGMENT_STATE_PIPELINED = 1 << 30;

    static final int SEGMENT_STATE_FINISHED = SEGMENT_STATE_PIPELINED - 1;

    /**
     * Internal state, not data
     */
    protected int index;

    public ResultHeader() {
        super();
    }

    public int columnCount() {
        return columns.length;
    }

    public ResultColumn getByIndex(int index) throws IndexOutOfBoundsException {
        return columns[index];
    }

    @Override
    public boolean isSegment() {
        return index < SEGMENT_STATE_FINISHED;
    }

    public void finish() {
        assert !hasNext();
        index = SEGMENT_STATE_FINISHED;
    }

    public boolean pipeline() {
        if (index == SEGMENT_STATE_FINISHED) {
            index = SEGMENT_STATE_PIPELINED;
            return true;
        } else {
            return false;
        }
    }

    @NotNull
    public ResultColumn getByVirtualName(@NotNull String name) throws MysqlException {
        for (ResultColumn column : columns) {
            if (column != null && name.equals(column.virtualName)) {
                return column;
            }
        }
        throw new MysqlException();
    }

    public boolean hasNext() {
        return index < columns.length;
    }

    @NotNull
    public ResultColumn next(@NotNull MysqlConnection context) {
        final ResultColumn column = new ResultColumn(context, index);
        columns[index++] = column;
        return column;
    }

    @Override
    public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        final long columnCount = buf.readLengthEncodedInteger();
        if (0 < columnCount && columnCount < MysqlByteBuf.MAX_SIZE) {
            columns = new ResultColumn[(int) columnCount];
            index = 0;
            buf.assertEnd();
        } else {
            throw new MysqlException();
        }
    }

    @Override
    public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        final int columnCount = columns.length;
        if (0 < columnCount && columnCount < MysqlByteBuf.MAX_SIZE) {
            buf.writeLengthEncodedInteger(columnCount);
            index = 0;
        } else {
            throw new MysqlException();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + Arrays.toString(columns);
    }

    /**
     * Created on 2024/5/21.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
     */
    public static class ResultTextHeader extends ResultHeader { }

    /**
     * Created on 2024/5/28.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
     */
    public static class ResultBinaryHeader extends ResultHeader { }

    /**
     * Created on 2024/5/22.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
     */
    public static class StatementPrepareOK extends ResultHeader {

        static final int PACKET_HEADER = OK.PACKET_HEADER;

        ResultColumn[] parameters; // parameter definition block first, then column definition block

        int statementId;

        int warningCount;

        boolean metadata;

        public int parameterCount() {
            return parameters.length;
        }

        public boolean hasNext() {
            return index < parameters.length + columns.length;
        }

        @NotNull
        public ResultColumn next(@NotNull MysqlConnection context) {
            final int parameterLength = parameters.length;
            ResultColumn column;
            if (index < parameterLength) {
                column = new ResultColumn(context, index);
                parameters[index++] = column;
            } else {
                column = new ResultColumn(context, index - parameterLength);
                columns[index++ - parameterLength] = column;
            }
            return column;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.assertEquals(PACKET_HEADER, buf.readInt1());
            statementId = buf.readInt4();
            final int columnCount = buf.readInt2();
            final int parameterCount = buf.readInt2();
            buf.assertEquals(0x00, buf.readInt1()); // filler
            if (buf.isReadable()) {
                warningCount = buf.readInt2();
                if (context.hasCapability(CapabilitiesFlags.OPTIONAL_RESULTSET_METADATA)) {
                    metadata = buf.readInt1() != 0;
                } else {
                    metadata = true;
                }
            }
            buf.assertEnd();
            parameters = new ResultColumn[parameterCount];
            columns = new ResultColumn[columnCount];
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.writeInt1(PACKET_HEADER);
            buf.writeInt4(statementId);
            buf.writeInt2(columns.length);
            buf.writeInt2(parameters.length);
            buf.writeInt1(0x00); // filler
        }
    }
}
