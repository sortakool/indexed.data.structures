package com.rsm.byteSlice;

/**
 * Created by Raymond on 12/18/13.
 */
public class ByteArraySlice {

    private byte[] data;
    private int offset;
    private int position;
    private int length;

    public ByteArraySlice() {
        this.data = null;
        this.offset = 0;
        this.position = 0;
        this.length = 0;
    }

    public ByteArraySlice(byte[] data) {
        this.data = data;
        this.offset = 0;
        this.position = 0;
        this.length = data.length;
    }

    public ByteArraySlice(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.position = 0;
        this.length = data.length;
    }

    public void set(byte[] data) {
        this.data = data;
        this.offset = 0;
        this.position = 0;
        this.length = data.length;
    }

    public void set(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.position = 0;
        this.length = data.length;
    }

    public byte[] getArray() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }

    public int getLength() {
        return length;
    }
}
