package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public class ShortColumnImpl extends BaseColumn<Short> implements ShortColumn {

    public ShortColumnImpl(String name) {
        this(name, DEFAULT_VALUE);
    }

    public ShortColumnImpl(String name, short defaultValue) {
        super(name, Type.SHORT, NumberOfBytes.SHORT, defaultValue);
    }
}
