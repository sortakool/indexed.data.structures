package com.rsm.tables.records;

/**
 * Created by Raymond on 12/18/13.
 */
public abstract class BaseRecord<TYPE> implements Record<TYPE> {

    protected TYPE key;

    public TYPE getKey() {
        return key;
    }
}
