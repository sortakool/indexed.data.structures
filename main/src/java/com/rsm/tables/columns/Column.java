package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public interface Column<DEFAULTVALUETYPE> {

    public enum Type {
        BYTE,
        BOOLEAN,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BYTES
    }

    public static final class NumberOfBytes {
        public static final int BYTE = Byte.SIZE/Byte.SIZE;
        public static final int BOOLEAN = Byte.SIZE/Byte.SIZE;
        public static final int SHORT = Short.SIZE/Byte.SIZE;
        public static final int INT = Integer.SIZE/Byte.SIZE;
        public static final int LONG = Long.SIZE/Byte.SIZE;
        public static final int FLOAT = Float.SIZE/Byte.SIZE;
        public static final int DOUBLE = Double.SIZE/Byte.SIZE;
    }

    public static final class Lengths {
        /**
         * The length for a {@link com.rsm.tables.columns.Column.Type#BYTES}
         * field is packed in the first 2 bytes (aka a short)
         * at the field's starting position
         */
        public static final int BYTES_VALUE_LENGTH = 2;
    }

    String getName();
    int getNumberOfBytes();
    Type getType();

    DEFAULTVALUETYPE getDefaultValue();
    void setDefaultValue(DEFAULTVALUETYPE value);
}
