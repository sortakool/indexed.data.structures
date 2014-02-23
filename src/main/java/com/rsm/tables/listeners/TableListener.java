package com.rsm.tables.listeners;

import com.rsm.tables.Table;
import com.rsm.tables.records.Record;

/**
 * Created by Raymond on 12/18/13.
 */
public interface TableListener<TABLE extends Table, RECORD extends Record> {

    void onAdd(RECORD record);
    void onUpdate(RECORD previousRecord, RECORD record);
    void onDelete(RECORD record);
}
