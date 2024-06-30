package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

/**
 * Created on 2024/6/20.
 */
public class MysqlProtocolException extends MysqlException {

    private static final long serialVersionUID = 0xBBEE63601D5F97F5L;

    final int code;

    public MysqlProtocolException(@NotNull ResponsePacket.Error error) {
        super(error.errorMessage);
        this.code = error.errorCode;
    }

    public int getCode() {
        return code;
    }
}
