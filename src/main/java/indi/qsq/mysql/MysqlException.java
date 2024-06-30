package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

/**
 * Created on 2024/5/22.
 */
public class MysqlException extends RuntimeException {

    private static final long serialVersionUID = 0xBB4FF3D6943544ADL;

    public MysqlException() {
        super();
    }

    public MysqlException(@NotNull String message) {
        super(message);
    }
}
