package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

/**
 * Created on 2024/5/20.
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
 */
public class ResultColumn extends ResponsePacket {

    static final String CATALOG = "def";

    @NotNull
    final MysqlConnection context;

    /**
     * Starts from zero.
     * Column index is not exposed to user, user get data from rows by passing column object as parameter, not index.
     */
    final int index;

    String schema;

    String virtualTable;

    String physicalTable;

    String virtualName;

    String physicalName;

    String defaultValues;

    /**
     * {@link CharacterSets}
     * Also called collation index
     */
    int characterSet;

    int columnLength;

    /**
     * {@link FieldTypes}
     */
    int type;

    /**
     * {@link ColumnFlags}
     */
    int flags;

    int decimals;

    public ResultColumn(@NotNull MysqlConnection context, int index) {
        super();
        this.context = context;
        this.index = index;
    }

    public boolean hasFlag(int flag) {
        return (this.flags & flag) != 0;
    }

    @Override
    public boolean isSegment() {
        return true;
    }

    @Override
    public void read(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
            buf.assertEquals(CATALOG, buf.readLengthEncodedString());
            schema = buf.readLengthEncodedString();
            virtualTable = buf.readLengthEncodedString();
            physicalTable = buf.readLengthEncodedString();
            virtualName = buf.readLengthEncodedString();
            physicalName = buf.readLengthEncodedString();
            buf.assertEquals(0x0c, buf.readLengthEncodedInteger()); // length of fixed length fields
            characterSet = buf.readInt2();
            columnLength = buf.readInt4();
            type = buf.readInt1();
            flags = buf.readInt2(); // always long flag
            decimals = buf.readInt1();
            buf.skipFixedLength(2); // zero padding
        } else {
            virtualTable = buf.readLengthEncodedString();
            virtualName = buf.readLengthEncodedString();
            buf.assertEquals(1, buf.readLengthEncodedInteger()); // length of type field
            type = buf.readInt1();
            if (context.hasCapability(CapabilitiesFlags.LONG_FLAG)) {
                buf.assertEquals(3, buf.readLengthEncodedInteger()); // length of flags + decimals fields
                flags = buf.readInt2();
            } else {
                buf.assertEquals(2, buf.readLengthEncodedInteger()); // length of flags + decimals fields
                flags = buf.readInt1();
            }
            decimals = buf.readInt1();
            if (context.getState() == ServerCommands.FIELD_LIST) {
                buf.assertEquals(2, buf.readLengthEncodedInteger()); // length of default values
                defaultValues = buf.readLengthEncodedString();
            }
        }
        buf.assertEnd();
    }

    @Override
    public void write(@NotNull MysqlByteBuf buf, @NotNull MysqlConnection context) {
        if (context.hasCapability(CapabilitiesFlags.PROTOCOL_41)) {
            buf.writeLengthEncodedString(CATALOG);
            buf.writeLengthEncodedString(schema);
            buf.writeLengthEncodedString(virtualTable);
            buf.writeLengthEncodedString(physicalTable);
            buf.writeLengthEncodedString(virtualName);
            buf.writeLengthEncodedString(physicalName);
            buf.writeLengthEncodedInteger(0x0c); // length of fixed length fields
            buf.writeInt2(characterSet);
            buf.writeInt4(columnLength);
            buf.writeInt1(type);
            buf.writeInt2(flags);
            buf.writeInt1(decimals);
        } else {
            buf.writeLengthEncodedString(virtualTable);
            buf.writeLengthEncodedString(virtualName);
            buf.writeLengthEncodedInteger(1);
            buf.writeInt1(type);
            if (context.hasCapability(CapabilitiesFlags.LONG_FLAG)) {
                buf.writeLengthEncodedInteger(3);
                buf.writeInt2(flags);
            } else {
                buf.writeLengthEncodedInteger(2);
                buf.writeInt1(flags);
            }
            buf.writeInt1(decimals);
            if (context.getState() == ServerCommands.FIELD_LIST) {
                buf.writeLengthEncodedInteger(2);
                buf.writeLengthEncodedString(defaultValues);
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "Column[schema = \"%s\", table = \"%s\" as \"%s\", name = \"%s\" as \"%s\", characterSet = %d, columnLength = %d, type = %d, flags = 0x%x, decimals = %d]",
                schema, physicalTable, virtualTable, physicalName, virtualName, characterSet, columnLength, type, flags, decimals
        );
    }
}
