package com.rsm.tables.records;

import com.rsm.buffer.BufferFacade;
import com.rsm.byteSlice.ByteArraySlice;

/**
 * Created by Raymond on 12/18/13.
 */
public interface BufferRecord<TYPE> extends Record<TYPE> {

    void initialize(int entry, TYPE key, BufferFacade buffer, int recordStartPosition);
//    ByteArraySlice getByteArraySlice();
    BufferFacade getBuffer();
}
