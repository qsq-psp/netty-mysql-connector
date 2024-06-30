package indi.qsq.mysql;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created on 2024/6/17.
 */
public class TextTypesTest {

    static final String TABLE_NAME = "text_types_test";

    static NioEventLoopGroup group;

    static MysqlConnection connection;

    @BeforeClass
    public static void connect() throws Exception {
        group = new NioEventLoopGroup(1);
        connection = MysqlConnection.create(group, Config.cachingSha2User()).sync().get();
        connection.execute(
                "create table if not exists " + TABLE_NAME + "("
                + "f_tinyint tinyint,"
                + "f_smallint smallint,"
                + "f_mediumint mediumint,"
                + "f_int int,"
                + "f_bigint bigint,"
                + "f_float float,"
                + "f_double double,"
                + "f_text text,"
                + "f_timestamp timestamp,"
                + "f_datetime datetime,"
                + "f_date date,"
                + "f_time time"
                + ");"
        ).sync();
    }

    @AfterClass
    public static void disconnect() throws Exception {
        if (connection == null) {
            return;
        }
        try {
            connection.execute("drop table if exists " + TABLE_NAME + ";").sync();
        } finally {
            try {
                connection.close().sync();
            } finally {
                connection = null;
                try {
                    group.shutdownGracefully().sync();
                } finally {
                    group = null;
                }
            }
        }
    }

    @Test
    public void testInteger() throws Exception {
        final ResultSet resultSet = connection.query("select 6 + 1, 4 - 22, 40021, 1999999999;").sync().get();
        assertEquals(4, resultSet.header.columnCount());
        assertEquals(1, resultSet.rows.size());
        // indexes start at 0
        final ResultRow row0 = resultSet.rows.get(0);
        assertEquals(6 + 1, row0.getInteger(resultSet.header.getByIndex(0)));
        assertEquals(4 - 22, row0.getInteger(resultSet.header.getByIndex(1)));
        assertEquals(40021, row0.getInteger(resultSet.header.getByIndex(2)));
        assertEquals(1999999999, row0.getInteger(resultSet.header.getByIndex(3)));
        resultSet.close();
    }

    @Test
    public void testString() throws Exception {
        final ResultSet resultSet = connection.query("select '\"double quoted\"', \"escaped\\\\\";").sync().get();
        assertEquals(2, resultSet.header.columnCount());
        assertEquals(1, resultSet.rows.size());
        // indexes start at 0
        final ResultRow row0 = resultSet.rows.get(0);
        assertEquals("\"double quoted\"", row0.getString(resultSet.header.getByIndex(0)));
        assertEquals("escaped\\", row0.getString(resultSet.header.getByIndex(1)));
        resultSet.close();
    }

    /**
     * The DATE type is used for values with a date part but no time part.
     * MySQL retrieves and displays DATE values in 'YYYY-MM-DD' format.
     */
    @Test
    public void testDate() throws Exception {
        final ResultSet resultSet = connection.query("select DATE'1000-01-01', DATE'9999-12-31';").sync().get();
        assertEquals(2, resultSet.header.columnCount());
        assertEquals(1, resultSet.rows.size());
        // indexes start at 0
        final ResultRow row0 = resultSet.rows.get(0);
        ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(FieldTypes.DATE, column.type);
        assertEquals("1000-01-01", row0.getString(column));
        assertEquals(1000, row0.getMysqlDateTime(column).year);
        assertEquals(1, row0.getMysqlDateTime(column).month);
        assertEquals(1, row0.getMysqlDateTime(column).date);
        column = resultSet.header.getByIndex(1);
        assertEquals(FieldTypes.DATE, column.type);
        assertEquals("9999-12-31", row0.getString(column));
        assertEquals(9999, row0.getMysqlDateTime(column).year);
        assertEquals(12, row0.getMysqlDateTime(column).month);
        assertEquals(31, row0.getMysqlDateTime(column).date);
        resultSet.close();
    }

    /**
     * The DATETIME type is used for values that contain both date and time parts.
     * MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD hh:mm:ss' format.
     * The TIMESTAMP data type is used for values that contain both date and time parts.
     */
    @Test
    public void testDateTime() throws Exception {
        final ResultSet resultSet = connection.query("select TIMESTAMP'1000-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59';").sync().get();
        assertEquals(2, resultSet.header.columnCount());
        assertEquals(1, resultSet.rows.size());
        // indexes start at 0
        final ResultRow row0 = resultSet.rows.get(0);
        ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(FieldTypes.DATE_TIME, column.type);
        assertEquals("1000-01-01 00:00:00", row0.getString(column));
        assertEquals(1, row0.getMysqlDateTime(column).date);
        assertEquals(0, row0.getMysqlDateTime(column).hour);
        column = resultSet.header.getByIndex(1);
        assertEquals(FieldTypes.DATE_TIME, column.type);
        assertEquals("9999-12-31 23:59:59", row0.getString(column));
        assertEquals(31, row0.getMysqlDateTime(column).date);
        assertEquals(23, row0.getMysqlDateTime(column).hour);
        assertEquals(59, row0.getMysqlDateTime(column).second);
        resultSet.close();
    }

    /**
     * As a string in 'D hh:mm:ss' format. You can also use one of the following “relaxed” syntaxes: 'hh:mm:ss', 'hh:mm', 'D hh:mm', 'D hh', or 'ss'.
     * Here D represents days and can have a value from 0 to 34.
     */
    @Test
    public void testTime() throws Exception {
        final ResultSet resultSet = connection.query("select TIME'02:00:00.999998', TIME'7 10:06', TIME'00:00:03.667';").sync().get();
        assertEquals(3, resultSet.header.columnCount());
        assertEquals(1, resultSet.rows.size());
        // indexes start at 0
        final ResultRow row0 = resultSet.rows.get(0);
        ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(FieldTypes.TIME, column.type);
        assertEquals("02:00:00.999998", row0.getString(column));
        assertEquals(0, row0.getMysqlTime(column).days);
        assertEquals(2, row0.getMysqlTime(column).hour);
        assertEquals(999998, row0.getMysqlTime(column).microsecond);
        column = resultSet.header.getByIndex(1);
        assertEquals(FieldTypes.TIME, column.type);
        assertEquals("178:06:00", row0.getString(column));
        assertEquals(6, row0.getMysqlTime(column).minute);
        assertEquals(0, row0.getMysqlTime(column).second);
        assertEquals(0, row0.getMysqlTime(column).microsecond);
        column = resultSet.header.getByIndex(2);
        assertEquals(3, row0.getMysqlTime(column).second);
        assertEquals(667000, row0.getMysqlTime(column).microsecond);
        resultSet.close();
    }

    @Test
    public void testTinyIntColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_tinyint) values (40), (127), (-111), (-128)").sync();
        final ResultSet resultSet = connection.query("select f_tinyint from " + TABLE_NAME + " where f_tinyint is not null order by f_tinyint asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_tinyint", column.physicalName);
        assertEquals(-128, resultSet.rows.get(0).getInteger(column));
        assertEquals(-111, resultSet.rows.get(1).getInteger(column));
        assertEquals(40, resultSet.rows.get(2).getInteger(column));
        assertEquals(127, resultSet.rows.get(3).getInteger(column));
        resultSet.close();
    }

    @Test
    public void testSmallIntColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_smallint) values (2024), (10086), (-32768), (0)").sync();
        final ResultSet resultSet = connection.query("select f_smallint from " + TABLE_NAME + " where f_smallint is not null order by f_smallint asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_smallint", column.physicalName);
        assertEquals(-32768, resultSet.rows.get(0).getInteger(column));
        assertEquals(0, resultSet.rows.get(1).getInteger(column));
        assertEquals(2024, resultSet.rows.get(2).getInteger(column));
        assertEquals(10086, resultSet.rows.get(3).getInteger(column));
        resultSet.close();
    }

    @Test
    public void testMediumIntColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_mediumint) values (50000), (6400), (-720000), (-1)").sync();
        final ResultSet resultSet = connection.query("select f_mediumint from " + TABLE_NAME + " where f_mediumint is not null order by f_mediumint asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_mediumint", column.physicalName);
        assertEquals(-720000, resultSet.rows.get(0).getInteger(column));
        assertEquals(-1, resultSet.rows.get(1).getInteger(column));
        assertEquals(6400, resultSet.rows.get(2).getInteger(column));
        assertEquals(50000, resultSet.rows.get(3).getInteger(column));
        resultSet.close();
    }

    @Test
    public void testIntColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_int) values (2147483647), (-2147483648), (-99), (1000000007)").sync();
        final ResultSet resultSet = connection.query("select f_int from " + TABLE_NAME + " where f_int is not null order by f_int asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_int", column.physicalName);
        assertEquals(-2147483648, resultSet.rows.get(0).getInteger(column));
        assertEquals(-99, resultSet.rows.get(1).getInteger(column));
        assertEquals(1000000007, resultSet.rows.get(2).getInteger(column));
        assertEquals(2147483647, resultSet.rows.get(3).getInteger(column));
        resultSet.close();
    }

    @Test
    public void testBigIntColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_bigint) values (0), (10), (-9223372036854775808), (9223372036854775807)").sync();
        final ResultSet resultSet = connection.query("select f_bigint from " + TABLE_NAME + " where f_bigint is not null order by f_bigint asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_bigint", column.physicalName);
        assertEquals(-9223372036854775808L, resultSet.rows.get(0).getLong(column));
        assertEquals(0L, resultSet.rows.get(1).getLong(column));
        assertEquals(10L, resultSet.rows.get(2).getLong(column));
        assertEquals(9223372036854775807L, resultSet.rows.get(3).getLong(column));
        resultSet.close();
    }

    @Test
    public void testFloatColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_float) values (208.4), (-130.18), (4.49), (82.2)").sync();
        final ResultSet resultSet = connection.query("select f_float from " + TABLE_NAME + " where f_float is not null order by f_float asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_float", column.physicalName);
        assertEquals(-130.18, resultSet.rows.get(0).getFloat(column), 0.001);
        assertEquals(4.49, resultSet.rows.get(1).getFloat(column), 0.001);
        assertEquals(82.2, resultSet.rows.get(2).getFloat(column), 0.001);
        assertEquals(208.4, resultSet.rows.get(3).getFloat(column), 0.001);
        resultSet.close();
    }

    @Test
    public void testDoubleColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_double) values (9.02), (-337.2), (608.33), (95.0)").sync();
        final ResultSet resultSet = connection.query("select f_double from " + TABLE_NAME + " where f_double is not null order by f_double asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_double", column.physicalName);
        assertEquals(-337.2, resultSet.rows.get(0).getFloat(column), 0.001);
        assertEquals(9.02, resultSet.rows.get(1).getFloat(column), 0.001);
        assertEquals(95.0, resultSet.rows.get(2).getFloat(column), 0.001);
        assertEquals(608.33, resultSet.rows.get(3).getFloat(column), 0.001);
        resultSet.close();
    }

    @Test
    public void testTextColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_text) values ('to'), ('board'), ('pleasure'), ('')").sync();
        final ResultSet resultSet = connection.query("select f_text from " + TABLE_NAME + " where f_text is not null order by length(f_text) asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_text", column.physicalName);
        assertEquals("", resultSet.rows.get(0).getString(column));
        assertEquals("to", resultSet.rows.get(1).getString(column));
        assertEquals("board", resultSet.rows.get(2).getString(column));
        assertEquals("pleasure", resultSet.rows.get(3).getString(column));
        resultSet.close();
    }

    @Test
    public void testTimestampColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_timestamp) values (TIMESTAMP'1970-07-01 02:02:02'), (TIMESTAMP'2038-01-05 01:59:07'), (TIMESTAMP'2030-03-10 14:06:22'), (DATE'2004-05-09')").sync();
        final ResultSet resultSet = connection.query("select f_timestamp from " + TABLE_NAME + " where f_timestamp is not null order by f_timestamp asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_timestamp", column.physicalName);
        assertEquals(1970, resultSet.rows.get(0).getMysqlDateTime(column).year);
        assertEquals(2004, resultSet.rows.get(1).getMysqlDateTime(column).year);
        assertEquals(2030, resultSet.rows.get(2).getMysqlDateTime(column).year);
        assertEquals(2038, resultSet.rows.get(3).getMysqlDateTime(column).year);
        resultSet.close();
    }

    @Test
    public void testDatetimeColumn() throws Exception {
        connection.execute("insert into " + TABLE_NAME + " (f_datetime) values (TIMESTAMP'1000-01-01 00:00:00'), (TIMESTAMP'9999-12-31 23:59:59'), (TIMESTAMP'2024-06-28 15:33:18'), (DATE'2000-01-01')").sync();
        final ResultSet resultSet = connection.query("select f_datetime from " + TABLE_NAME + " where f_datetime is not null order by f_datetime asc;").sync().get();
        assertEquals(1, resultSet.header.columnCount());
        assertEquals(4, resultSet.rows.size());
        final ResultColumn column = resultSet.header.getByIndex(0);
        assertEquals(TABLE_NAME, column.physicalTable);
        assertEquals("f_datetime", column.physicalName);
        assertEquals(1000, resultSet.rows.get(0).getMysqlDateTime(column).year);
        assertEquals(2000, resultSet.rows.get(1).getMysqlDateTime(column).year);
        assertEquals(2024, resultSet.rows.get(2).getMysqlDateTime(column).year);
        assertEquals(9999, resultSet.rows.get(3).getMysqlDateTime(column).year);
        resultSet.close();
    }

    public void testDateColumn() {
        //
    }

    public void testTimeColumn() {
        //
    }
}
