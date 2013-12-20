package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class DoubleColumnImpl extends BaseColumn<Double> implements DoubleColumn {

    public DoubleColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public DoubleColumnImpl(String name, double defaultValue) {
        super(name, Type.DOUBLE, NumberOfBytes.DOUBLE, defaultValue);
    }
}
