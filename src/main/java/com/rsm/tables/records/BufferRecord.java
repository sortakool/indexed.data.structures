package com.rsm.tables.records;

import com.rsm.buffer.Bytes;

/**
 * Created by Raymond on 12/18/13.
 */
public interface BufferRecord<TYPE> extends Record<TYPE> {

    void initialize(int entry, TYPE key, Bytes buffer, int recordStartPosition);
//    ByteArraySlice getByteArraySlice();
    Bytes getBuffer();
}
