package indi.qsq.mysql;

/**
 * Created on 2024/5/21.
 *
 * com.mysql.cj.MysqlType
 */
public interface FieldTypes {

    int DECIMAL = 0;
    int TINY = 1;
    int SHORT = 2;
    int LONG = 3;
    int FLOAT = 4;
    int DOUBLE = 5;
    int NULL = 6;
    int TIMESTAMP = 7;
    int LONG_LONG = 8;
    int INT24 = 9;
    int DATE = 10;
    int TIME = 11;
    int DATE_TIME = 12;
    int YEAR = 13;
    int VAR_CHAR = 15;
    int BIT = 16;
    int JSON = 245;
    int NEW_DECIMAL = 246;
    int ENUM = 247;
    int SET = 248;
    int TINY_BLOB = 249;
    int MEDIUM_BLOB = 250;
    int LONG_BLOB = 251;
    int BLOB = 252;
    int VAR_STRING = 253;
    int STRING = 254;
    int GEOMETRY = 255;
}
