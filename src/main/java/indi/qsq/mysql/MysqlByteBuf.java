package indi.qsq.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.DefaultByteBufHolder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Created on 2024/5/19.
 */
public class MysqlByteBuf extends DefaultByteBufHolder {

    static final int MAX_SIZE = 0x40000;

    public MysqlByteBuf(ByteBuf data) {
        super(data);
    }

    @Override
    @NotNull
    public MysqlByteBuf replace(ByteBuf content) {
        return new MysqlByteBuf(content);
    }

    @Override
    @NotNull
    public MysqlByteBuf retain() {
        super.retain();
        return this;
    }

    public boolean isReadable() {
        return content().isReadable();
    }

    @NotNull
    public MysqlByteBuf allocateBuffer() {
        return new MysqlByteBuf(content().alloc().buffer());
    }

    @NotNull
    public MysqlByteBuf readSlice(int length) {
        if (length < 0) {
            throw new RuntimeException();
        }
        final ByteBuf data = content();
        final ByteBuf slice = data.alloc().buffer();
        data.readBytes(slice, length);
        return new MysqlByteBuf(slice);
    }

    @NotNull
    public MysqlByteBuf readSlice(long length) {
        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException();
        }
        return readSlice((int) length);
    }

    public int readInt1() {
        return content().readUnsignedByte();
    }

    public void writeInt1(int value) {
        content().writeByte(value);
    }

    public int readInt2() {
        return content().readUnsignedShortLE();
    }

    public void writeInt2(int value) {
        content().writeShortLE(value);
    }

    public int readInt3() {
        final ByteBuf data = content();
        int b0 = data.readByte();
        int b1 = data.readByte();
        int b2 = data.readByte();
        return ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | (b0 & 0xff);
    }

    public void writeInt3(int value) {
        final ByteBuf data = content();
        data.writeByte(value);
        data.writeByte(value >> 8);
        data.writeByte(value >> 16);
    }

    public int readInt4() {
        return content().readIntLE();
    }

    public void writeInt4(int value) {
        content().writeIntLE(value);
    }

    public long readInt8() {
        return content().readLongLE();
    }

    public void writeInt8(long value) {
        content().writeLongLE(value);
    }

    public long readLengthEncodedInteger() {
        final ByteBuf data = content();
        final int first = data.readUnsignedByte();
        switch (first) {
            default:
                return first;
            case 0xfb:
            case 0xfc:
                return data.readUnsignedShortLE();
            case 0xfd: {
                int b0 = data.readByte();
                int b1 = data.readByte();
                int b2 = data.readByte();
                return ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | (b0 & 0xff);
            }
            case 0xfe:
                return data.readLongLE();
            case 0xff:
                throw new RuntimeException();
        }
    }

    public void writeLengthEncodedInteger(long value) {
        final ByteBuf data = content();
        if (value >= 0) {
            if (value < 0xfb) {
                data.writeByte((byte) value);
                return;
            } else if (value < 0x10000) {
                data.writeByte((byte) 0xfc);
                data.writeShortLE((int) value);
                return;
            } else if (value < 0x1000000) {
                data.writeByte((byte) 0xfd);
                data.writeByte((byte) value);
                data.writeByte((byte) (value >> 8));
                data.writeByte((byte) (value >> 16));
                return;
            }
        }
        data.writeByte((byte) 0xfe);
        data.writeLongLE(value);
    }

    public void skipFixedLength(int length) {
        content().skipBytes(length);
    }

    public void skipFixedLength(long length) {
        if (!(0 <= length && length < Integer.MAX_VALUE)) {
            throw new RuntimeException();
        }
        content().skipBytes((int) length);
    }

    @NotNull
    public byte[] readFixedLengthBytes(int length) {
        final byte[] array = new byte[length];
        content().readBytes(array);
        return array;
    }

    public void writeFixedLengthBytes(@NotNull byte[] bytes) {
        content().writeBytes(bytes);
    }

    public String readFixedLengthString(int length) {
        return content().readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    public int writeFixedLengthString(@NotNull String string) {
        return content().writeCharSequence(string, StandardCharsets.UTF_8);
    }

    @NotNull
    public byte[] readNullTerminatedBytes() {
        final ByteBuf data = content();
        final int length = data.bytesBefore((byte) 0);
        if (length == -1) {
            throw new RuntimeException();
        }
        final byte[] bytes = readFixedLengthBytes(length);
        data.readByte();
        return bytes;
    }

    public void writeNullTerminatedBytes(@NotNull byte[] bytes) {
        for (int value : bytes) {
            if (value == 0) {
                throw new RuntimeException();
            }
        }
        final ByteBuf data = content();
        data.writeBytes(bytes);
        data.writeByte(0);
    }

    public String readNullTerminatedString() {
        final ByteBuf data = content();
        final int length = data.bytesBefore((byte) 0);
        if (length == -1) {
            throw new RuntimeException();
        }
        final String string = data.readCharSequence(length, StandardCharsets.UTF_8).toString();
        data.readByte();
        return string;
    }

    public void writeNullTerminatedString(@NotNull String string) {
        final ByteBuf data = content();
        final int index = data.writerIndex();
        data.writeCharSequence(string, StandardCharsets.UTF_8);
        if (data.bytesBefore(index, data.writerIndex() - index, (byte) 0) != -1) {
            throw new RuntimeException();
        }
        data.writeByte(0);
    }

    @NotNull
    public byte[] readLengthEncodedBytes() {
        final long length = readLengthEncodedInteger();
        if (!(0 <= length && length < MAX_SIZE)) {
            throw new RuntimeException();
        }
        return readFixedLengthBytes((int) length);
    }

    public void writeLengthEncodedBytes(@Nullable byte[] bytes) {
        if (bytes != null) {
            writeLengthEncodedInteger(bytes.length);
            writeFixedLengthBytes(bytes);
        } else {
            writeInt1(0);
        }
    }

    public void writeLengthEncodedBytes(@NotNull ByteBuf bytes) {
        writeLengthEncodedInteger(bytes.readableBytes());
        content().writeBytes(bytes);
    }

    public void writeLengthEncodedBytes(@NotNull ByteBufHolder bytes) {
        writeLengthEncodedBytes(bytes.content());
    }

    @NotNull
    public String readLengthEncodedHexDump() {
        final long longLength = readLengthEncodedInteger();
        if (!(0 <= longLength && longLength < MAX_SIZE)) {
            throw new RuntimeException();
        }
        final int length = (int) longLength;
        final ByteBuf data = content();
        final int start = data.readerIndex();
        final String hexDump = ByteBufUtil.hexDump(data, start, length);
        data.readerIndex(data.readerIndex() + length);
        return hexDump;
    }

    @NotNull
    public String readLengthEncodedString() {
        final long length = readLengthEncodedInteger();
        if (!(0 <= length && length < MAX_SIZE)) {
            throw new RuntimeException();
        }
        return readFixedLengthString((int) length);
    }

    public void writeLengthEncodedString(String string) {
        if (string != null) {
            writeLengthEncodedBytes(string.getBytes(StandardCharsets.UTF_8));
        } else {
            writeInt1(0);
        }
    }

    @NotNull
    public byte[] readRestOfPacketBytes() {
        final ByteBuf data = content();
        final byte[] array = new byte[data.readableBytes()];
        data.readBytes(array);
        return array;
    }

    @NotNull
    public String readRestOfPacketString() {
        final ByteBuf data = content();
        return data.readCharSequence(data.readableBytes(), StandardCharsets.UTF_8).toString();
    }

    public int getInt1() {
        final ByteBuf data = content();
        return data.getUnsignedByte(data.readerIndex());
    }

    public void assertEnd() {
        assert !content().isReadable();
    }

    public <T> void assertPredicate(@NotNull Predicate<T> expected, T actual) {
        assert expected.test(actual);
    }

    public void assertEquals(int expected, int actual) {
        assert expected == actual;
    }

    public void assertEquals(long expected, long actual) {
        assert expected == actual;
    }

    public void assertEquals(byte[] expected, byte[] actual) {
        assert Arrays.equals(expected, actual);
    }

    public void assertEquals(@NotNull String expected, String actual) {
        assert expected.equals(actual);
    }
}
