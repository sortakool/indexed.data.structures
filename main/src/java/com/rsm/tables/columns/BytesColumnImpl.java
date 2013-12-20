package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class BytesColumnImpl extends BaseColumn<byte[]> implements BytesColumn {

    public BytesColumnImpl(String name, int numberOfBytes) {
        this(name, numberOfBytes, DEFAULT_VALUE);
    }

    public BytesColumnImpl(String name, int numberOfBytes, byte[] defaultValue) {
        super(name, Type.BYTES, numberOfBytes, defaultValue);
    }
}
