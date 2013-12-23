package com.rsm.tables;

import com.rsm.tables.columns.Column;
import com.rsm.tables.listeners.TableListener;
import com.rsm.tables.records.Record;
import com.rsm.tables.schemas.Schema;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Raymond on 12/19/13.
 */
public abstract class BaseTable<RECORD extends Record> implements Table<RECORD> {

    public static final int DEFAULT_POSITION = -1;

    protected Set<TableListener> tableListeners = new HashSet<>(2);
    protected final Set<SchemaInitializer> schemaInitializers;
    protected Schema schema;
    protected int size = 0;
    protected int rowLength = 0;
    protected Object2IntOpenHashMap column2Position;
    protected boolean initialized = false;

    public BaseTable(Set<SchemaInitializer> schemaInitializers) {
        this.schemaInitializers = schemaInitializers;
        schema = createSchema();
        notifySchemaInitializers();
        this.size = 0;
        init();
    }

    protected void init() {
        column2Position = new Object2IntOpenHashMap(schema.getColumns().size(), 1.0f);
        column2Position.defaultReturnValue(DEFAULT_POSITION);;
        for (Column column : schema.getColumns()) {
            column2Position.put(column, rowLength);
            rowLength += column.getNumberOfBytes();
            if(column.getType() == Column.Type.BYTES) {
                rowLength += Column.Lengths.BYTES_VALUE_LENGTH;
            }
        }
        this.schema.init();
        this.initialized = true;
    }


    protected abstract Schema createSchema();

    public Set<TableListener> getTableListeners() {
        return tableListeners;
    }

    public Set<SchemaInitializer> getSchemaInitializers() {
        return schemaInitializers;
    }

    public Schema getSchema() {
        return schema;
    }

    protected void notifySchemaInitializers() {
        if(schemaInitializers != null) {
            for (SchemaInitializer schemaInitializer : schemaInitializers) {
                schemaInitializer.initialize(schema);
            }
        }
    }

    @Override
    public void setListeners(Set<TableListener> listeners) {
        this.tableListeners = listeners;
    }

    @Override
    public void addListener(TableListener listener) {
        if(tableListeners != null) {
            tableListeners.add(listener);
        }
    }

    @Override
    public void removeListener(TableListener listener) {
        if(tableListeners != null) {
            tableListeners.remove(listener);
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    public boolean isInitialized() {
        return initialized;
    }
}
