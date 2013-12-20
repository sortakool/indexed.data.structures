package com.rsm.tables.cursors;

import com.rsm.tables.records.Record;

/**
 * Created by Raymond on 12/19/13.
 */
public interface Cursor<RECORD extends Record> {

    boolean hasNext();
    void next(RECORD destination);
}
