package com.rsm.tables;

import com.rsm.tables.listeners.TableListener;
import com.rsm.tables.records.Record;
import com.rsm.tables.schemas.Schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Raymond on 12/19/13.
 */
public abstract class BaseTable<RECORD extends Record> implements Table<RECORD> {

    protected Set<TableListener> tableListeners = new HashSet<>(2);
    protected final Set<SchemaInitializer> schemaInitializers;
    protected Schema schema;

    public BaseTable(Set<SchemaInitializer> schemaInitializers) {
        this.schemaInitializers = schemaInitializers;
        schema = createSchema();
        notifySchemaInitializers();
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
}
