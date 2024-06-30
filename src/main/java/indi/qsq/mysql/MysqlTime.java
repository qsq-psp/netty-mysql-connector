package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created on 2024/5/28.
 */
public class MysqlTime extends PacketObject {

    boolean isNegative;

    int days;

    int hour;

    int minute;

    int second;

    int microsecond;

    public MysqlTime() {
        super();
    }

    public void set(@NotNull MysqlTime that) {
        this.days = that.days;
        this.hour = that.hour;
        this.minute = that.minute;
        this.second = that.second;
        this.microsecond = that.microsecond;
    }

    private static final Pattern PATTERN = Pattern.compile("^(\\d+):(\\d+):(\\d+)(\\.\\d+)?$");

    /**
     * @param string do not mark it @NotNull, expect it to throw NullPointerException
     */
    public void parse(CharSequence string) throws NullPointerException, NumberFormatException {
        final Matcher matcher = PATTERN.matcher(string);
        if (matcher.matches()) {
            hour = Integer.parseInt(matcher.group(1));
            minute = Integer.parseInt(matcher.group(2));
            second = Integer.parseInt(matcher.group(3));
            if (matcher.start(4) != matcher.end(4)) {
                microsecond = ResultRow.parseMantissaInt(string, matcher.start(4) + 1, matcher.end(4), 6); // 10^6 us = 1 s
            } else {
                microsecond = 0;
            }
        } else {
            throw new NumberFormatException();
        }
    }

    /**
     * It's static so we can skip a binary mysql time without creating a instance
     */
    public static void skip(@NotNull MysqlByteBuf buf) throws MysqlException {
        final int length = buf.readInt1();
        switch (length) {
            case 0:
            case 8:
            case 12:
                buf.skipFixedLength(length);
                break;
            default:
                throw new MysqlException();
        }
    }

    public void read(@NotNull MysqlByteBuf buf) throws MysqlException {
        final int length = buf.readInt1();
        if (length == 0) {
            isNegative = false;
            days = 0;
            hour = 0;
            minute = 0;
            second = 0;
            microsecond = 0;
        } else if (length == 8) {
            isNegative = buf.readInt1() != 0;
            days = buf.readInt4();
            hour = buf.readInt1();
            minute = buf.readInt1();
            second = buf.readInt1();
            microsecond = 0;
        } else if (length == 12) {
            isNegative = buf.readInt1() != 0;
            days = buf.readInt4();
            hour = buf.readInt1();
            minute = buf.readInt1();
            second = buf.readInt1();
            microsecond = buf.readInt4();
        } else {
            throw new MysqlException();
        }
    }

    @Override
    public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        read(buf);
    }

    @Override
    public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        if (microsecond == 0) {
            if (second == 0 && minute == 0 && hour == 0 && days == 0) {
                buf.writeInt1(0); // length
            } else {
                buf.writeInt1(8); // length
                buf.writeInt1(isNegative ? 1 : 0);
                buf.writeInt4(days);
                buf.writeInt1(hour);
                buf.writeInt1(minute);
                buf.writeInt1(second);
            }
        } else {
            buf.writeInt1(12); // length
            buf.writeInt1(isNegative ? 1 : 0);
            buf.writeInt4(days);
            buf.writeInt1(hour);
            buf.writeInt1(minute);
            buf.writeInt1(second);
            buf.writeInt1(microsecond);
        }
    }
}
