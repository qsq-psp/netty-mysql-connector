package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

/**
 * Created on 2024/5/30.
 */
public class PacketObject {

    public PacketObject() {
        super();
    }

    public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        // pass
    }

    public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        // pass
    }
}
