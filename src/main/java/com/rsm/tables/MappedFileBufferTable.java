package com.rsm.tables;

import com.rsm.buffer.Bytes;
import com.rsm.buffer.MappedFileBuffer;
import com.rsm.tables.records.BufferRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Set;


/**
 * Created by Raymond on 12/19/13.
 */
public abstract class MappedFileBufferTable<RECORD extends BufferRecord> extends BaseTable<RECORD> implements Table<RECORD> {

    protected Logger log = LogManager.getLogger(getClass());

    protected boolean initialized;
    protected long size = 0;
    protected float loadFactor;
    protected long expectedCapacity;
    protected long capacity;

    protected Bytes buffer;

    protected File file;
    protected int segmentSize;
    protected long initialFileSize;
    protected long growBySize;
    protected boolean readWrite;
    protected boolean deleteFileIfExists;


    public MappedFileBufferTable(float loadFactor, long expectedCapacity, Set<SchemaInitializer> schemaInitializers,
                                 File file, int segmentSize, long initialFileSize, long growBySize, boolean deleteFileIfExists) throws IOException {
        super(schemaInitializers);
        this.loadFactor = loadFactor;
        this.expectedCapacity = expectedCapacity;
        this.buffer = new MappedFileBuffer(file, segmentSize, initialFileSize, growBySize, true, deleteFileIfExists);
    }

}
