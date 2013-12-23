package com.rsm.tables.records;

import com.rsm.buffer.BufferFacade;
import com.rsm.byteSlice.ByteArraySlice;
import com.rsm.tables.columns.*;
import com.rsm.tables.schemas.Schema;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.Serializable;

/**
 * Created by Raymond on 12/18/13.
 */
public class BufferBaseRecordImpl<TYPE> extends BaseRecord<TYPE> implements BufferRecord<TYPE> {

    protected static final int INVALID_POSIITON = -1;

    protected Schema schema;
    protected boolean exists;
    protected int entry;
//    protected final ByteArraySlice dataSlice = new ByteArraySlice();
    protected BufferFacade buffer;
    protected Object2IntOpenHashMap column2Position;
    protected int recordStartPosition;

    protected BufferBaseRecordImpl(Schema schema, Object2IntOpenHashMap column2Position) {
        this.schema = schema;
        this.column2Position = column2Position;
        init();
    }

    protected void init() {

    }

    @Override
    public void initialize(int entry, TYPE key, BufferFacade buffer, int recordStartPosition) {
        this.entry = entry;
        this.key = key;
        this.buffer = buffer;
        this.exists = true;
        this.recordStartPosition = recordStartPosition;
    }

    @Override
    public BufferFacade getBuffer() {
        return buffer;
    }

    public boolean exists() {
        return exists;
    }

    @Override
    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public int getEntry() {
        return entry;
    }

    protected long getPosition(Column column) {
        return recordStartPosition + getColumnOffset(column);
    }

    protected long getColumnOffset(Column column) {
        int position = column2Position.getInt(column);
        if(position == INVALID_POSIITON) {
            throw new IllegalStateException("Position in column2Position for column: " + column + " is -1");
        }
        return position;
    }



    @Override
    public boolean getBoolean(BooleanColumn column) {
        return (BooleanColumn.TRUE_VALUE == buffer.get(getPosition(column)));
    }

    @Override
    public void setBoolean(BooleanColumn column, boolean value) {
        if(value) {
            buffer.put(getPosition(column), BooleanColumn.TRUE_VALUE);
        }
        else {
            buffer.put(getPosition(column), BooleanColumn.FALSE_VALUE);
        }
    }

    @Override
    public byte getByte(ByteColumn column) {
        return buffer.get(getPosition(column));
    }

    @Override
    public void setByte(ByteColumn column, byte value) {
        buffer.put(getPosition(column), value);
    }

    @Override
    public short getShort(ShortColumn column) {
        return buffer.getShort(getPosition(column));
    }

    @Override
    public void setShort(ShortColumn column, short value) {
        buffer.putShort(getPosition(column), value);
    }

    @Override
    public int getInt(IntColumn column) {
        return buffer.getInt(getPosition(column));
    }

    @Override
    public void setInt(IntColumn column, int value) {
        buffer.putInt(getPosition(column), value);
    }

    @Override
    public long getLong(LongColumn column) {
        return buffer.getLong(getPosition(column));
    }

    @Override
    public void setLong(LongColumn column, long value) {
        buffer.putLong(getPosition(column), value);
    }

    @Override
    public float getFloat(FloatColumn column) {
        return buffer.getFloat(getPosition(column));
    }

    @Override
    public void setFloat(FloatColumn column, float value) {
        buffer.putFloat(getPosition(column), value);
    }

    @Override
    public double getDouble(DoubleColumn column) {
        return buffer.getDouble(getPosition(column));
    }

    @Override
    public void setDouble(DoubleColumn column, double value) {
        buffer.putDouble(getPosition(column), value);
    }

    @Override
    /**
     * TODO handle situation where position is greater than Integer.MAX_VALUE
     */
    public ByteArraySlice getBytes(BytesColumn column, ByteArraySlice destination) {
        long position = getPosition(column);
        int length = buffer.getShort(position);
        int dataPosition = (int) (position + Column.Lengths.BYTES_VALUE_LENGTH);
        destination.set(buffer.getArray(), dataPosition, length);
        return destination;
    }

    @Override
    public void getBytes(BytesColumn column, byte[] destination, int offset) {
        long position = getPosition(column);
        int length = buffer.getShort(position);
        int dataPosition = (int) (position + Column.Lengths.BYTES_VALUE_LENGTH);
        System.arraycopy(buffer.getArray(), dataPosition, destination, offset, length);
    }

    @Override
    public String getBytes(BytesColumn column) {
        long position = getPosition(column);
        int length = buffer.getShort(position);
        int dataPosition = (int) (position + Column.Lengths.BYTES_VALUE_LENGTH);
        return new String(buffer.getArray(), dataPosition, length);
    }

    @Override
    public void setBytes(BytesColumn column, String value) {
        long position = getPosition(column);
        buffer.putBytes(position, value.getBytes(), 0, value.getBytes().length);
    }

    @Override
    public void setBytes(BytesColumn column, ByteArraySlice value) {
        long position = getPosition(column);
        buffer.putBytes(position, value.getArray(), value.getPosition(), value.getLength());
    }

    @Override
    public void setBytes(BytesColumn column, byte[] value) {
        setBytes(column, value, 0, value.length);
    }

    @Override
    public void setBytes(BytesColumn column, byte[] value, int offset, int length) {
        long position = getPosition(column);
        buffer.putBytes(position, value, offset, length);
    }

    @Override
    public Serializable getObject(ObjectColumn column) {
        return null;
    }

    @Override
    public void setObject(ObjectColumn column) {

    }

    @Override
    public void deltaByte(ByteColumn column, byte delta) {
        long position = getPosition(column);
        byte value = (byte)(buffer.get(position) + delta);
        buffer.put(position, value);
    }

    @Override
    public void deltaShort(ShortColumn column, short delta) {
        long position = getPosition(column);
        short value = (short)(buffer.getShort(position) + delta);
        buffer.putShort(position, value);
    }

    @Override
    public void deltaInt(IntColumn column, int delta) {
        long position = getPosition(column);
        int value = (int)(buffer.getInt(position) + delta);
        buffer.putInt(position, value);
    }

    @Override
    public void deltaLong(ByteColumn column, long delta) {
        long position = getPosition(column);
        long value = (long)(buffer.getInt(position) + delta);
        buffer.putLong(position, value);
    }

    @Override
    public void deltaFloat(FloatColumn column, float delta) {
        long position = getPosition(column);
        float value = (float)(buffer.getFloat(position) + delta);
        buffer.putFloat(position, value);
    }

    @Override
    public void deltaDouble(DoubleColumn column, double delta) {
        long position = getPosition(column);
        double value = (double)(buffer.getDouble(position) + delta);
        buffer.putDouble(position, value);
    }
}
