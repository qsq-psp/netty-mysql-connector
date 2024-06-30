package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created on 2024/5/28.
 */
public class MysqlDateTime extends PacketObject {

    public int year;

    public int month;

    public int date;

    public int hour;

    public int minute;

    public int second;

    public int microsecond;

    public MysqlDateTime() {
        super();
    }

    public void set(@NotNull MysqlDateTime that) {
        this.year = that.year;
        this.month = that.month;
        this.date = that.date;
        this.hour = that.hour;
        this.minute = that.minute;
        this.second = that.second;
        this.microsecond = that.microsecond;
    }

    public void setDateTime(@NotNull LocalDateTime localDateTime) {
        setDate(localDateTime);
        setTime(localDateTime);
    }

    public void setDate(@NotNull LocalDateTime localDateTime) {
        year = localDateTime.getYear();
        month = localDateTime.getMonthValue(); // month in mysql and LocalDateTime both starts at 1
        date = localDateTime.getDayOfMonth(); // date in mysql and LocalDateTime both starts at 1, but in LocalDateTime it is called day of month
    }

    public void setTime(@NotNull LocalDateTime localDateTime) {
        hour = localDateTime.getHour();
        minute = localDateTime.getMinute();
        second = localDateTime.getSecond();
        microsecond = localDateTime.getNano() / 1000; // 1us = 1000ns
    }

    public void setDate(@NotNull LocalDate localDate) {
        year = localDate.getYear();
        month = localDate.getMonthValue(); // month in mysql and LocalDate both starts at 1
        date = localDate.getDayOfMonth(); // date in mysql and LocalDate both starts at 1, but in LocalDate it is called day of month
    }

    public void setTime(@NotNull LocalTime localTime) {
        hour = localTime.getHour();
        minute = localTime.getMinute();
        second = localTime.getSecond();
        microsecond = localTime.getNano() / 1000; // 1us = 1000ns
    }

    public void getDateTime(@NotNull Calendar calendar) {
        getDate(calendar);
        getTime(calendar);
    }

    @SuppressWarnings("MagicConstant")
    public void getDate(@NotNull Calendar calendar) {
        calendar.set(year, month - 1, date); // month in mysql starts at 1, but month in Calendar starts at 0
    }

    public void getTime(@NotNull Calendar calendar) {
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
    }

    public void setDateTime(@NotNull Calendar calendar) throws MysqlException {
        setDate(calendar);
        setTime(calendar);
    }

    public void setDate(@NotNull Calendar calendar) throws MysqlException {
        if (calendar.get(Calendar.ERA) != GregorianCalendar.AD) { // BC is not supported
            throw new MysqlException();
        }
        year = calendar.get(Calendar.YEAR);
        month = calendar.get(Calendar.MONTH) + 1; // month in mysql starts at 1, but month in Calendar starts at 0
        date = calendar.get(Calendar.DATE); // date in mysql and Calendar both starts at 1
    }

    public void setTime(@NotNull Calendar calendar) {
        hour = calendar.get(Calendar.HOUR_OF_DAY); // Calendar.HOUR is for the 12-hour clock (0-11)
        minute = calendar.get(Calendar.MINUTE);
        second = calendar.get(Calendar.SECOND);
        microsecond = 1000 * calendar.get(Calendar.MILLISECOND); // 1000 us = 1 ms
    }

    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+)-(\\d+)-(\\d+)( (\\d+):(\\d+):(\\d+)(\\.\\d+)?)?\\s*$");

    /**
     * @param string do not mark it @NotNull, expect it to throw NullPointerException
     */
    public void parse(CharSequence string) throws NullPointerException, NumberFormatException {
        final Matcher matcher = PATTERN.matcher(string);
        if (matcher.matches()) {
            year = Integer.parseInt(matcher.group(1));
            month = Integer.parseInt(matcher.group(2));
            date = Integer.parseInt(matcher.group(3));
            if (matcher.start(4) != matcher.end(4)) {
                hour = Integer.parseInt(matcher.group(5));
                minute = Integer.parseInt(matcher.group(6));
                second = Integer.parseInt(matcher.group(7));
                if (matcher.start(8) != matcher.end(8)) {
                    microsecond = ResultRow.parseMantissaInt(string, matcher.start(8) + 1, matcher.end(8), 6); // 10^6 us = 1 s
                } else {
                    microsecond = 0;
                }
            } else {
                hour = 0;
                minute = 0;
                second = 0;
            }
        } else {
            throw new NumberFormatException();
        }
    }

    /**
     * It's static so we can skip a binary mysql date time without creating a instance
     */
    public static void skip(@NotNull MysqlByteBuf buf) throws MysqlException {
        final int length = buf.readInt1();
        switch (length) {
            case 0:
            case 4:
            case 7:
            case 11:
                buf.skipFixedLength(length);
                break;
            default:
                throw new MysqlException();
        }
    }

    public void read(@NotNull MysqlByteBuf buf) throws MysqlException {
        final int length = buf.readInt1();
        if (length == 0) {
            year = 0;
            month = 0;
            date = 0;
            hour = 0;
            minute = 0;
            second = 0;
            microsecond = 0;
        } else if (length == 4) {
            year = buf.readInt2();
            month = buf.readInt1();
            date = buf.readInt1();
            hour = 0;
            minute = 0;
            second = 0;
            microsecond = 0;
        } else if (length == 7) {
            year = buf.readInt2();
            month = buf.readInt1();
            date = buf.readInt1();
            hour = buf.readInt1();
            minute = buf.readInt1();
            second = buf.readInt1();
            microsecond = 0;
        } else if (length == 11) {
            year = buf.readInt2();
            month = buf.readInt1();
            date = buf.readInt1();
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
            if (second == 0 && minute == 0 && hour == 0) {
                if (date == 0 && month == 0 && year == 0) {
                    buf.writeInt1(0); // length
                } else {
                    buf.writeInt1(4); // length
                    buf.writeInt2(year);
                    buf.writeInt1(month);
                    buf.writeInt1(date);
                }
            } else {
                buf.writeInt1(7); // length
                buf.writeInt2(year);
                buf.writeInt1(month);
                buf.writeInt1(date);
                buf.writeInt1(hour);
                buf.writeInt1(minute);
                buf.writeInt1(second);
            }
        } else {
            buf.writeInt1(11); // length
            buf.writeInt2(year);
            buf.writeInt1(month);
            buf.writeInt1(date);
            buf.writeInt1(hour);
            buf.writeInt1(minute);
            buf.writeInt1(second);
            buf.writeInt4(microsecond);
        }
    }

    @Override
    public int hashCode() {
        return (((((((year * 0x5f) + month) * 0x5f) + date) * 0x5f + hour) * 0x5f + minute) * 0x5f + second) * 0x5f + microsecond;
    }

    public boolean dateTimeEquals(@NotNull MysqlDateTime that) {
        return this.dateEquals(that) && this.timeEquals(that);
    }

    public boolean dateEquals(@NotNull MysqlDateTime that) {
        return this.year == that.year && this.month == that.month && this.date == that.date;
    }

    public boolean timeEquals(@NotNull MysqlDateTime that) {
        return this.hour == that.hour && this.minute == that.minute && this.second == that.second
                && this.microsecond == that.microsecond;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof MysqlDateTime && dateTimeEquals((MysqlDateTime) obj);
    }

    @NotNull
    public String dateToString() {
        return String.format(
                "%04d-%02d-%02d",
                year, month, date
        );
    }

    @NotNull
    public String timeToString() {
        return String.format(
                "%02d:%02d:%02d.%06d",
                hour, minute, second, microsecond
        );
    }

    @NotNull
    public String dateTimeToString() {
        return String.format(
                "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                year, month, date, hour, minute, second, microsecond
        );
    }

    @Override
    public String toString() {
        return dateTimeToString();
    }
}
