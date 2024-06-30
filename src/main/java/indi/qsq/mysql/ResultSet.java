package indi.qsq.mysql;

import io.netty.util.ReferenceCountUtil;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * Created on 2024/5/20.
 */
public class ResultSet implements MysqlImmediateCloseable {

    @NotNull
    final ResultHeader header;

    @NotNull
    final ArrayList<ResultRow> rows;

    ResponsePacket.EOF end;

    public ResultSet(@NotNull ResultHeader header, @NotNull ArrayList<ResultRow> rows) {
        super();
        this.header = header;
        this.rows = rows;
    }

    public ResultSet(@NotNull ResultHeader header) {
        this(header, new ArrayList<>());
    }

    @Override
    public void close() {
        final int size = rows.size();
        for (int i = 0; i < size; i++) {
            ReferenceCountUtil.release(rows.set(i, null));
        }
    }

    @Override
    public String toString() {
        return String.format("ResultSet[header = %s, rows.length = %d, end = %s]", header, rows.size(), end);
    }
}
