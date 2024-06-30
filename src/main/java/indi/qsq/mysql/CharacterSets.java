package indi.qsq.mysql;

/**
 * Created on 2024/5/20.
 *
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_character_set.html
 */
public interface CharacterSets {

    /**
     * Default for latin1
     */
    int LATIN1_SWEDISH_CI = 8;

    int LATIN1_BIN = 47;

    /**
     * Default for UTF-8 max bytes 3
     */
    int UTF8_GENERAL_CI = 33;

    int UTF8_BIN = 83;

    int UTF8_UNICODE_CI = 192;

    /**
     * Default for UTF-8 max bytes 4
     */
    int UTF8MB4_0900_AI_CI = 255;

    int UTF8MB4_0900_BIN = 309;

    int UTF8MB4_GENERAL_CI = 45;

    int UTF8MB4_BIN = 46;

    int UTF8MB4_UNICODE_CI = 224;

    /**
     * Default for UTF-16
     */
    int UTF16_GENERAL_CI = 54;

    int UTF16_BIN = 55;

    /**
     * Default for UTF-16 little endian
     */
    int UTF16LE_GENERAL_CI = 56;

    int UTF16LE_BIN = 62;

    /**
     * Default for UTF-32
     */
    int UTF32_GENERAL_CI = 60;

    int UTF32_BIN = 61;

    int UTF32_UNICODE_CI = 160;
}
