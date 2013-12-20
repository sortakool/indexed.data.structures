package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class LongColumnImpl extends BaseColumn<Long> implements LongColumn {

    public LongColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public LongColumnImpl(String name, long defaultValue) {
        super(name, Type.LONG, NumberOfBytes.LONG, defaultValue);
    }
}
