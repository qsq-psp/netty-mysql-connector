package indi.qsq.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created on 2024/5/20.
 */
public abstract class ResultRow extends ResponsePacket {

    static final int TEXT_NULL = 0xfb;

    static final int BINARY_NULL = -1;

    public abstract boolean isBinary();

    public abstract boolean isMultithreadSafe();

    public abstract boolean isNull(@NotNull ResultColumn column) throws MysqlException;

    public abstract String getString(@NotNull ResultColumn column) throws MysqlException;

    public abstract int getInteger(@NotNull ResultColumn column) throws MysqlException, NullPointerException;

    public Integer getBoxedInteger(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        } else {
            return getInteger(column);
        }
    }

    public abstract long getLong(@NotNull ResultColumn column) throws MysqlException, NullPointerException;

    public Long getBoxedLong(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        } else {
            return getLong(column);
        }
    }

    public abstract float getFloat(@NotNull ResultColumn column) throws MysqlException, NullPointerException;

    public Float getBoxedFloat(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        } else {
            return getFloat(column);
        }
    }

    public abstract double getDouble(@NotNull ResultColumn column) throws MysqlException, NullPointerException;

    public Double getBoxedDouble(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        } else {
            return getDouble(column);
        }
    }

    public abstract void getMysqlDateTime(@NotNull ResultColumn column, @NotNull MysqlDateTime value) throws MysqlException, NullPointerException;

    public MysqlDateTime getMysqlDateTime(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        }
        final MysqlDateTime value = new MysqlDateTime();
        getMysqlDateTime(column, value);
        return value;
    }

    public abstract void getMysqlTime(@NotNull ResultColumn column, @NotNull MysqlTime value) throws MysqlException, NullPointerException;

    public MysqlTime getMysqlTime(@NotNull ResultColumn column) throws MysqlException {
        if (isNull(column)) {
            return null;
        }
        final MysqlTime value = new MysqlTime();
        getMysqlTime(column, value);
        return value;
    }

    public static abstract class Decoded<T> extends ResultRow implements List<T>, Cloneable, Serializable {

        private static final long serialVersionUID = 0xDCDBCA6473A1D9A4L;

        protected T[] values;

        @Override
        public boolean isMultithreadSafe() {
            return true;
        }

        @Override
        public boolean isNull(@NotNull ResultColumn column) {
            return values[column.index] == null;
        }

        @Override
        public String getString(@NotNull ResultColumn column) {
            final Object object = values[column.index];
            if (object != null) {
                return object.toString();
            } else {
                return null;
            }
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public boolean isEmpty() {
            return values.length != 0;
        }

        @Override
        public boolean contains(Object element) {
            for (Object item : values) {
                if (Objects.equals(item, element)) {
                    return true;
                }
            }
            return false;
        }

        @NotNull
        @Override
        public Iterator<T> iterator() {
            return new ArrayIndexIterator();
        }

        @NotNull
        @Override
        public Object[] toArray() {
            return values;
        }

        @SuppressWarnings("unchecked")
        @NotNull
        @Override
        public <A> A[] toArray(@NotNull A[] typeArray) {
            final int length = values.length;
            if (typeArray.length < length) {
                typeArray = (A[]) Array.newInstance(typeArray.getClass().getComponentType(), length);
            }
            for (int index = 0; index < length; index++) {
                typeArray[index] = (A) values[index];
            }
            if (length < typeArray.length) {
                typeArray[length] = null;
            }
            return typeArray;
        }

        @Override
        public boolean add(T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> collection) {
            for (Object element : collection) {
                if (!contains(element)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends T> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, @NotNull Collection<? extends T> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T get(int index) {
            return values[index];
        }

        @Override
        public T set(int index, T element) {
            final T oldElement = values[index];
            values[index] = element;
            return oldElement;
        }

        @Override
        public void add(int index, T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public T remove(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int indexOf(Object element) {
            final int length = values.length;
            for (int index = 0; index < length; index++) {
                if (Objects.equals(values[index], element)) {
                    return index;
                }
            }
            return -1;
        }

        @Override
        public int lastIndexOf(Object element) {
            for (int index = values.length - 1; index >= 0; index--) {
                if (Objects.equals(values[index], element)) {
                    return index;
                }
            }
            return -1;
        }

        @NotNull
        @Override
        public ListIterator<T> listIterator() {
            return new ArrayIndexIterator();
        }

        @NotNull
        @Override
        public ListIterator<T> listIterator(int index) {
            return new ArrayIndexIterator(index);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object that) {
            return this == that || this.getClass() == that.getClass() && Arrays.equals(this.values, ((Decoded) that).values);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + Arrays.toString(values);
        }

        /**
         * Created on 2024/5/20.
         */
        class ArrayIndexIterator implements ListIterator<T> {

            int index;

            ArrayIndexIterator() {
                super();
            }

            ArrayIndexIterator(int index) {
                super();
                this.index = index;
            }

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public T next() {
                return values[index++];
            }

            @Override
            public boolean hasPrevious() {
                return index > 0;
            }

            @Override
            public T previous() {
                return values[--index];
            }

            @Override
            public int nextIndex() {
                return index;
            }

            @Override
            public int previousIndex() {
                return index - 1;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void set(T t) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(T t) {
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Created on 2024/5/20.
     */
    public static abstract class Raw extends ResultRow implements ByteBufHolder {

        protected MysqlByteBuf buf;

        @Override
        public boolean isMultithreadSafe() {
            return false;
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            this.buf = buf.retain();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            buf.content().writeBytes(buf.content());
        }

        @Override
        public ByteBuf content() {
            return buf.content();
        }

        @Override
        public Raw copy() {
            return replace((MysqlByteBuf) buf.copy());
        }

        @Override
        public ByteBufHolder duplicate() {
            return replace((MysqlByteBuf) buf.duplicate());
        }

        @Override
        public ByteBufHolder retainedDuplicate() {
            return replace((MysqlByteBuf) buf.retainedDuplicate());
        }

        @Override
        public Raw replace(ByteBuf dataContent) {
            return replace(new MysqlByteBuf(dataContent));
        }

        public abstract Raw replace(MysqlByteBuf directContent);

        @Override
        public int refCnt() {
            return buf.refCnt();
        }

        @Override
        public Raw retain() {
            buf.retain();
            return this;
        }

        @Override
        public Raw retain(int increment) {
            buf.retain(increment);
            return this;
        }

        @Override
        public Raw touch() {
            buf.touch();
            return this;
        }

        @Override
        public Raw touch(Object hint) {
            buf.touch(hint);
            return this;
        }

        @Override
        public boolean release() {
            return buf.release();
        }

        @Override
        public boolean release(int decrement) {
            return buf.release(decrement);
        }
    }

    /**
     * Created on 2024/5/22.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
     * A row with data for each column.
     * NULL is sent as 0xFB
     * everything else is converted to a string and is sent as string(lenenc)
     */
    public static class DecodedText extends Decoded<String> {

        DecodedText() {
            super();
        }

        @Override
        public boolean isBinary() {
            return false;
        }

        @Override
        public int getInteger(@NotNull ResultColumn column) throws NullPointerException {
            final String string = values[column.index];
            if (string == null) {
                throw new NullPointerException();
            }
            if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                return Integer.parseUnsignedInt(string); // throws NumberFormatException instead of NullPointerException internally
            } else {
                return Integer.parseInt(string); // throws NumberFormatException instead of NullPointerException internally
            }
        }

        @Override
        public long getLong(@NotNull ResultColumn column) throws NullPointerException {
            final String string = values[column.index];
            if (string == null) {
                throw new NullPointerException();
            }
            if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                return Long.parseUnsignedLong(string); // throws NumberFormatException instead of NullPointerException internally
            } else {
                return Long.parseLong(string); // throws NumberFormatException instead of NullPointerException internally
            }
        }

        @Override
        public float getFloat(@NotNull ResultColumn column) throws NullPointerException {
            return Float.parseFloat(values[column.index]); // throws NullPointerException internally
        }

        @Override
        public double getDouble(@NotNull ResultColumn column) throws NullPointerException {
            return Double.parseDouble(values[column.index]); // throws NullPointerException internally
        }

        @Override
        public void getMysqlDateTime(@NotNull ResultColumn column, @NotNull MysqlDateTime value) throws NullPointerException {
            value.parse(values[column.index]); // throws NullPointerException
        }

        @Override
        public void getMysqlTime(@NotNull ResultColumn column, @NotNull MysqlTime value) throws NullPointerException {
            value.parse(values[column.index]); // throws NullPointerException
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            final int length = context.resultHeader.columns.length;
            values = new String[length];
            for (int index = 0; index < length; index++) {
                if (buf.getInt1() == TEXT_NULL) {
                    buf.readInt1();
                } else {
                    values[index] = buf.readLengthEncodedString();
                }
            }
            buf.assertEnd();
        }

        @Override
        public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            for (String value : values) {
                if (value == null) {
                    buf.writeInt1(TEXT_NULL);
                } else {
                    buf.writeLengthEncodedString(value);
                }
            }
        }

        @NotNull
        @Override
        public DecodedText subList(int fromIndex, int toIndex) {
            final DecodedText replaced = new DecodedText();
            replaced.values = Arrays.copyOfRange(values, fromIndex, toIndex);
            return replaced;
        }
    }

    /**
     * Created on 2024/5/20.
     */
    public static class RawText extends Raw {

        RawText() {
            super();
        }

        @Override
        public boolean isBinary() {
            return false;
        }

        @Override
        public boolean isMultithreadSafe() {
            return true;
        }

        @Override
        public boolean isNull(@NotNull ResultColumn column) throws MysqlException {
            final ByteBuf indirectContent = content();
            final int start = indirectContent.readerIndex();
            try {
                for (int index = column.index; index > 0; index--) {
                    if (buf.getInt1() == TEXT_NULL) {
                        buf.readInt1();
                    } else {
                        buf.skipFixedLength(buf.readLengthEncodedInteger());
                    }
                }
                return buf.getInt1() == TEXT_NULL;
            } finally {
                indirectContent.readerIndex(start);
            }
        }

        @Override
        public String getString(@NotNull ResultColumn column) throws MysqlException {
            final ByteBuf indirectContent = content();
            final int start = indirectContent.readerIndex();
            try {
                for (int index = column.index; index > 0; index--) {
                    if (buf.getInt1() == TEXT_NULL) {
                        buf.readInt1();
                    } else {
                        buf.skipFixedLength(buf.readLengthEncodedInteger());
                    }
                }
                if (buf.getInt1() == TEXT_NULL) {
                    return null;
                } else {
                    return buf.readLengthEncodedString();
                }
            } finally {
                indirectContent.readerIndex(start);
            }
        }

        @Override
        public int getInteger(@NotNull ResultColumn column) throws NullPointerException {
            final String string = getString(column);
            if (string == null) {
                throw new NullPointerException();
            }
            if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                return Integer.parseUnsignedInt(string); // throws NumberFormatException instead of NullPointerException internally
            } else {
                return Integer.parseInt(string); // throws NumberFormatException instead of NullPointerException internally
            }
        }

        @Override
        public long getLong(@NotNull ResultColumn column) throws NullPointerException {
            final String string = getString(column);
            if (string == null) {
                throw new NullPointerException();
            }
            if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                return Long.parseUnsignedLong(string); // throws NumberFormatException instead of NullPointerException internally
            } else {
                return Long.parseLong(string); // throws NumberFormatException instead of NullPointerException internally
            }
        }

        @Override
        public float getFloat(@NotNull ResultColumn column) throws NullPointerException {
            return Float.parseFloat(getString(column)); // throws NullPointerException internally
        }

        @Override
        public double getDouble(@NotNull ResultColumn column) throws NullPointerException {
            return Double.parseDouble(getString(column)); // throws NullPointerException internally
        }

        @Override
        public void getMysqlDateTime(@NotNull ResultColumn column, @NotNull MysqlDateTime value) throws NullPointerException {
            value.parse(getString(column)); // throws NullPointerException
        }

        @Override
        public void getMysqlTime(@NotNull ResultColumn column, @NotNull MysqlTime value) throws NullPointerException {
            value.parse(getString(column)); // throws NullPointerException
        }

        @Override
        public RawText replace(MysqlByteBuf directContent) {
            final RawText replaced = new RawText();
            replaced.buf = directContent;
            return replaced;
        }
    }

    /**
     * Created on 2024/5/22.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
     * For the Binary Protocol Resultset Row the num_fields and the field_pos need to add a offset of 2. For COM_STMT_EXECUTE this offset is 0.
     */
    public static class DecodedBinary extends Decoded<Object> {

        DecodedBinary() {
            super();
        }

        @Override
        public boolean isBinary() {
            return true;
        }

        @Override
        public int getInteger(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public long getLong(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public float getFloat(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof Number) {
                return ((Number) value).floatValue();
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public double getDouble(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public void getMysqlDateTime(@NotNull ResultColumn column, @NotNull MysqlDateTime mysqlDateTime) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof MysqlDateTime) {
                mysqlDateTime.set((MysqlDateTime) value);
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public MysqlDateTime getMysqlDateTime(@NotNull ResultColumn column) throws MysqlException {
            final Object value = values[column.index];
            if (value instanceof MysqlDateTime) {
                return (MysqlDateTime) value;
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public void getMysqlTime(@NotNull ResultColumn column, @NotNull MysqlTime mysqlTime) throws MysqlException, NullPointerException {
            final Object value = values[column.index];
            if (value instanceof MysqlTime) {
                mysqlTime.set((MysqlTime) value);
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public MysqlTime getMysqlTime(@NotNull ResultColumn column) throws MysqlException {
            final Object value = values[column.index];
            if (value instanceof MysqlTime) {
                return (MysqlTime) value;
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            final ResultColumn[] columns = context.resultHeader.columns;
            final int columnCount = columns.length;
            if (values == null || values.length != columnCount) {
                values = new Object[columnCount];
            }
            final ByteBuf data = buf.content();
            int byteIndex = data.readerIndex();
            int bitIndex = 2; // beginning two bits are reserved for future use
            buf.skipFixedLength((columnCount + 9) / Byte.SIZE); // null map length
            for (int index = 0; index < columnCount; index++) {
                boolean isNull = (data.getByte(byteIndex) & (1 << bitIndex)) != 0;
                if (++bitIndex == Byte.SIZE) {
                    byteIndex++;
                    bitIndex = 0;
                }
                ResultColumn column = columns[index];
                if (isNull) {
                    if (column.hasFlag(ColumnFlags.NOT_NULL)) { // contradiction
                        throw new MysqlException();
                    }
                    values[index] = null;
                    continue;
                }
                switch (column.type) {
                    case FieldTypes.TINY:
                        values[index] = (byte) buf.readInt1(); // convert to integer later
                        break;
                    case FieldTypes.SHORT:
                    case FieldTypes.YEAR:
                        values[index] = (short) buf.readInt2(); // convert to integer later
                        break;
                    case FieldTypes.INT24:
                    case FieldTypes.LONG:
                        values[index] = buf.readInt4();
                        break;
                    case FieldTypes.LONG_LONG:
                        values[index] = buf.readInt8();
                        break;
                    case FieldTypes.FLOAT:
                        values[index] = Float.intBitsToFloat(buf.readInt4());
                        break;
                    case FieldTypes.DOUBLE:
                        values[index] = Double.longBitsToDouble(buf.readInt8());
                        break;
                    case FieldTypes.VAR_CHAR:
                    case FieldTypes.VAR_STRING:
                    case FieldTypes.STRING:
                    case FieldTypes.ENUM:
                    case FieldTypes.SET:
                    case FieldTypes.GEOMETRY:
                    case FieldTypes.BIT:
                    case FieldTypes.DECIMAL:
                    case FieldTypes.NEW_DECIMAL:
                        values[index] = buf.readLengthEncodedString();
                        break;
                    case FieldTypes.TINY_BLOB:
                    case FieldTypes.MEDIUM_BLOB:
                    case FieldTypes.LONG_BLOB:
                    case FieldTypes.BLOB:
                        values[index] = buf.readLengthEncodedBytes();
                        break;
                    case FieldTypes.DATE:
                    case FieldTypes.DATE_TIME:
                    case FieldTypes.TIMESTAMP: {
                        MysqlDateTime value = new MysqlDateTime();
                        value.read(buf);
                        values[index] = value;
                        break;
                    }
                    case FieldTypes.TIME: {
                        MysqlTime value = new MysqlTime();
                        value.read(buf);
                        values[index] = value;
                        break;
                    }
                    default:
                        throw new MysqlException();
                }
            }
            buf.assertEnd();
        }

        @NotNull
        @Override
        public DecodedBinary subList(int fromIndex, int toIndex) {
            final DecodedBinary replaced = new DecodedBinary();
            replaced.values = Arrays.copyOfRange(values, fromIndex, toIndex);
            return replaced;
        }
    }

    /**
     * Created on 2024/5/22.
     */
    public static class RawBinary extends Raw {

        RawBinary() {
            super();
        }

        int[] positions;

        void locate(@NotNull ResultColumn column) throws NullPointerException {
            final int position = positions[column.index];
            if (position == BINARY_NULL) {
                throw new NullPointerException();
            } else {
                content().readerIndex(position);
            }
        }

        @Override
        public boolean isBinary() {
            return true;
        }

        @Override
        public boolean isNull(@NotNull ResultColumn column) throws MysqlException {
            return positions[column.index] == BINARY_NULL;
        }

        @Override
        public String getString(@NotNull ResultColumn column) throws MysqlException {
            final int position = positions[column.index];
            if (position == BINARY_NULL) {
                return null;
            } else {
                content().readerIndex(position);
            }
            switch (column.type) {
                case FieldTypes.VAR_CHAR:
                case FieldTypes.VAR_STRING:
                case FieldTypes.STRING:
                case FieldTypes.ENUM:
                case FieldTypes.SET:
                case FieldTypes.GEOMETRY:
                case FieldTypes.BIT:
                case FieldTypes.DECIMAL:
                case FieldTypes.NEW_DECIMAL:
                    return buf.readLengthEncodedString();
                case FieldTypes.TINY_BLOB:
                case FieldTypes.MEDIUM_BLOB:
                case FieldTypes.LONG_BLOB:
                case FieldTypes.BLOB:
                    return buf.readLengthEncodedHexDump();
                default:
                    return null; // todo
            }
        }

        @Override
        public int getInteger(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            locate(column);
            switch (column.type) {
                case FieldTypes.TINY:
                    if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                        return buf.readInt1();
                    } else {
                        return (byte) buf.readInt1();
                    }
                case FieldTypes.SHORT:
                case FieldTypes.YEAR:
                    if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                        return buf.readInt2();
                    } else {
                        return (short) buf.readInt2();
                    }
                case FieldTypes.LONG:
                case FieldTypes.INT24:
                    return buf.readInt4();
                default:
                    throw new MysqlException();
            }
        }

        @Override
        public long getLong(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            locate(column);
            switch (column.type) {
                case FieldTypes.TINY:
                    if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                        return buf.readInt1();
                    } else {
                        return (byte) buf.readInt1();
                    }
                case FieldTypes.SHORT:
                case FieldTypes.YEAR:
                    if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                        return buf.readInt2();
                    } else {
                        return (short) buf.readInt2();
                    }
                case FieldTypes.LONG:
                case FieldTypes.INT24:
                    if (column.hasFlag(ColumnFlags.UNSIGNED)) {
                        return 0xffffffffL & buf.readInt4();
                    } else {
                        return buf.readInt4();
                    }
                case FieldTypes.LONG_LONG:
                    return buf.readInt8();
                default:
                    throw new MysqlException();
            }
        }

        @Override
        public float getFloat(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            locate(column);
            switch (column.type) {
                case FieldTypes.FLOAT:
                    return Float.intBitsToFloat(buf.readInt4());
                case FieldTypes.DOUBLE:
                    return (float) Double.longBitsToDouble(buf.readInt8());
                case FieldTypes.DECIMAL:
                case FieldTypes.NEW_DECIMAL:
                    return Float.parseFloat(buf.readLengthEncodedString());
                default:
                    throw new MysqlException();
            }
        }

        @Override
        public double getDouble(@NotNull ResultColumn column) throws MysqlException, NullPointerException {
            locate(column);
            switch (column.type) {
                case FieldTypes.FLOAT:
                    return Float.intBitsToFloat(buf.readInt4());
                case FieldTypes.DOUBLE:
                    return Double.longBitsToDouble(buf.readInt8());
                case FieldTypes.DECIMAL:
                case FieldTypes.NEW_DECIMAL:
                    return Double.parseDouble(buf.readLengthEncodedString());
                default:
                    throw new MysqlException();
            }
        }

        @Override
        public void getMysqlDateTime(@NotNull ResultColumn column, @NotNull MysqlDateTime value) throws MysqlException, NullPointerException {
            content().readerIndex(positions[column.index]);
            switch (column.type) {
                case FieldTypes.DATE:
                case FieldTypes.DATE_TIME:
                case FieldTypes.TIMESTAMP:
                    value.read(buf);
                    break;
                default:
                    throw new MysqlException();
            }
        }

        @Override
        public void getMysqlTime(@NotNull ResultColumn column, @NotNull MysqlTime value) throws MysqlException, NullPointerException {
            content().readerIndex(positions[column.index]);
            if (column.type == FieldTypes.TIME) {
                value.read(buf);
            } else {
                throw new MysqlException();
            }
        }

        @Override
        public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
            super.read(buf, context);
            final ResultColumn[] columns = context.resultHeader.columns;
            final int columnCount = columns.length;
            final int nullMapLength = (columnCount + 9) / Byte.SIZE;
            final ByteBuf data = buf.content();
            positions = new int[columnCount];
            {
                int byteIndex = data.readerIndex();
                int bitIndex = 2; // beginning two bits are reserved for future use
                for (int index = 0; index < columnCount; index++) {
                    if ((data.getByte(byteIndex) & (1 << bitIndex)) != 0) {
                        positions[index] = -1; // -1 means null
                    }
                    if (++bitIndex == Byte.SIZE) {
                        byteIndex++;
                        bitIndex = 0;
                    }
                }
            }
            buf.skipFixedLength(nullMapLength);
            for (int index = 0; index < columnCount; index++) {
                ResultColumn column = columns[index];
                if (positions[index] == -1) {
                    if (column.hasFlag(ColumnFlags.NOT_NULL)) { // contradiction
                        throw new MysqlException();
                    }
                    continue;
                }
                positions[index] = data.readerIndex();
                switch (column.type) {
                    case FieldTypes.TINY:
                        buf.skipFixedLength(1);
                        break;
                    case FieldTypes.SHORT:
                    case FieldTypes.YEAR:
                        buf.skipFixedLength(2);
                        break;
                    case FieldTypes.LONG:
                    case FieldTypes.INT24:
                    case FieldTypes.FLOAT:
                        buf.skipFixedLength(4);
                        break;
                    case FieldTypes.LONG_LONG:
                    case FieldTypes.DOUBLE:
                        buf.skipFixedLength(8);
                        break;
                    case FieldTypes.VAR_CHAR:
                    case FieldTypes.VAR_STRING:
                    case FieldTypes.STRING:
                    case FieldTypes.ENUM:
                    case FieldTypes.SET:
                    case FieldTypes.GEOMETRY:
                    case FieldTypes.BIT:
                    case FieldTypes.DECIMAL:
                    case FieldTypes.NEW_DECIMAL:
                    case FieldTypes.TINY_BLOB:
                    case FieldTypes.MEDIUM_BLOB:
                    case FieldTypes.LONG_BLOB:
                    case FieldTypes.BLOB:
                        buf.skipFixedLength(buf.readLengthEncodedInteger());
                        break;
                    case FieldTypes.DATE:
                    case FieldTypes.DATE_TIME:
                    case FieldTypes.TIMESTAMP:
                        MysqlDateTime.skip(buf);
                        break;
                    case FieldTypes.TIME:
                        MysqlTime.skip(buf);
                        break;
                    default:
                        throw new MysqlException();
                }
            }
            buf.assertEnd();
        }

        @Override
        public RawBinary replace(@NotNull MysqlByteBuf directContent) {
            final RawBinary replaced = new RawBinary();
            replaced.buf = directContent;
            return replaced;
        }
    }

    public static int parseInt(CharSequence string, int fromIndex, int toIndex) throws NumberFormatException {
        return Integer.parseInt(string, fromIndex, toIndex, 10); // always decimal
    }

    private static final int[] TENS = {
            1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000
    };

    public static int parseMantissaInt(CharSequence string, int fromIndex, int toIndex, int magnitude) throws NumberFormatException {
        int value = 0;
        for (int index = fromIndex; index < toIndex; index++) {
            int digit = string.charAt(index) - '0';
            if (0 < digit && digit < 10) {
                value += digit * TENS[--magnitude];
            } else if (digit != 0) {
                throw new NumberFormatException();
            }
        }
        return value;
    }
}
