package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class IntColumnImpl extends BaseColumn<Integer> implements IntColumn {

    public IntColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public IntColumnImpl(String name, int defaultValue) {
        super(name, Type.INT, NumberOfBytes.INT, defaultValue);
    }
}
