package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class BooleanColumnImpl extends BaseColumn<Boolean> implements BooleanColumn {

    public BooleanColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public BooleanColumnImpl(String name, boolean defaultValue) {
        super(name, Type.BOOLEAN, NumberOfBytes.BOOLEAN, defaultValue);
    }
}
