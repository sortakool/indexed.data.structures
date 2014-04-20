package com.rsm.util;

/**
 * Created by rmanaloto on 4/19/14.
 */
public enum ByteUnit {
    BYTE(1),
    KILOBYTE(1024),
    MEGABYTE(1024*1024),
    GIGABYTE(1024*1024*1024),
    TERABYTE(1024*1024*1024*1024),
    PETABYTE(1024*1024*1024*1024*1024)
    ;

    private final long bytes;

    ByteUnit(long bytes) {
        this.bytes = bytes;
    }

    public long getBytes() {
        return bytes;
    }
}
