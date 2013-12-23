package com.rsm.tables.schemas;

import com.rsm.tables.columns.Column;

import java.util.Collection;

/**
 * Created by Raymond on 12/18/13.
 */
public interface Schema {

    boolean isInitialized();
    void init();

    Collection<Column> getColumns();
    void addColumns(Column... columns);

    Column getColumnByName(String columnName);
}
