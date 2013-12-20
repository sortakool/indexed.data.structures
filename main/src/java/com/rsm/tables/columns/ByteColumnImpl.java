package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class ByteColumnImpl extends BaseColumn<Byte> implements ByteColumn {

    public ByteColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public ByteColumnImpl(String name, byte defaultValue) {
        super(name, Type.BYTE, NumberOfBytes.BYTE, defaultValue);
    }
}
