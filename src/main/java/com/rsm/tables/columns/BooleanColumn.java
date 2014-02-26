package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public interface BooleanColumn extends Column<Boolean> {

    byte TRUE_VALUE = '1';
    byte FALSE_VALUE = '0';

    boolean DEFAULT_VALUE = false;
}
