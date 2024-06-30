package indi.qsq.mysql;

/**
 * Created on 2024/5/22.
 *
 * https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
 */
@SuppressWarnings("PointlessBitwiseExpression")
public interface ColumnFlags {

    int NOT_NULL = 1 << 0;

    int PRIMARY_KEY = 1 << 1;

    int UNIQUE_KEY = 1 << 2;

    /**
     * Field is part of a key.
     */
    int MULTIPLE_KEY = 1 << 3;

    int BLOB = 1 << 4;

    int UNSIGNED = 1 << 5;

    int ZERO_FILL = 1 << 6;

    int BINARY = 1 << 7;

    int ENUM = 1 << 8;

    int AUTO_INCREMENT = 1 << 9;

    int TIMESTAMP = 1 << 10;

    int SET = 1 << 11;

    int NO_DEFAULT_VALUE = 1 << 12;

    int ON_UPDATE_NOW = 1 << 13;

    int NUMBER = 1 << 14;

    /**
     * Intern; Part of some key.
     */
    int PART_KEY = 1 << 15;

    int GROUP = 1 << 16;

    /**
     * Field is explicitly specified as NULL by the user.
     */
    int EXPLICIT_NULL = 1 << 27;

    /**
     * 	Field will not be loaded in secondary engine.
     */
    int NOT_SECONDARY = 1 << 29;

    /**
     * Field is explicitly marked as invisible by the user.
     */
    int INVISIBLE = 1 << 30;
}
