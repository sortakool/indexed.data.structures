package com.rsm.tables;

import com.rsm.tables.cursors.Cursor;
import com.rsm.tables.listeners.TableListener;
import com.rsm.tables.records.Record;
import com.rsm.tables.schemas.Schema;

import java.util.Collection;
import java.util.Set;

/**
 * Created by Raymond on 12/18/13.
 */
public interface Table<RECORD extends Record> {

    Schema getSchema();

    int getSize();
    int getCapacity();
    int getExpectedCapacity();

    void getByEntry(int entry, RECORD record);

    void setListeners(Set<TableListener> listeners);
    void addListener(TableListener listeners);
    void removeListener(TableListener listeners);

    Cursor<RECORD> generateCursor();
    void initializeCursor(Cursor<RECORD> cursor);

    RECORD generateRecord();

    void preUpdate(RECORD record);
    void postUpdate(RECORD record);
}

