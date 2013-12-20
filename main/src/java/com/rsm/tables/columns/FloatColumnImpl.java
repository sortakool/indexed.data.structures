package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class FloatColumnImpl extends BaseColumn<Float> implements FloatColumn {

    public FloatColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public FloatColumnImpl(String name, float defaultValue) {
        super(name, Type.FLOAT, NumberOfBytes.FLOAT, defaultValue);
    }
}
