package com.rsm.tables.records;

import com.rsm.byteSlice.ByteArraySlice;
import com.rsm.tables.columns.BooleanColumn;
import com.rsm.tables.columns.ByteColumn;
import com.rsm.tables.columns.IntColumn;
import com.rsm.tables.columns.ShortColumn;
import com.rsm.tables.columns.*;

import java.io.Serializable;

/**
 * Created by Raymond on 12/18/13.
 */
public interface Record<TYPE> {

    TYPE getKey();

    void setExists(boolean exists);
    boolean exists();

    int getEntry();

    boolean getBoolean(BooleanColumn column);
    void setBoolean(BooleanColumn column, boolean value);

    byte getByte(ByteColumn column);
    void setByte(ByteColumn column, byte value);

    short getShort(ShortColumn column);
    void setShort(ShortColumn column, short value);

    int getInt(IntColumn column);
    void setInt(IntColumn column, int value);

    long getLong(LongColumn column);
    void setLong(LongColumn column, long value);

    float getFloat(FloatColumn column);
    void setFloat(FloatColumn column, float value);

    double getDouble(DoubleColumn column);
    void setDouble(DoubleColumn column, double value);

    ByteArraySlice getBytes(BytesColumn column, ByteArraySlice destination);
    void getBytes(BytesColumn column, byte[] column, int offset);
    String getBytes(BytesColumn column);
    void setBytes(BytesColumn column, String value);
    void setBytes(BytesColumn column, ByteArraySlice value);
    void setBytes(BytesColumn column, byte[] value);
    void setBytes(BytesColumn column, byte[] value, int offset, int length);

    Serializable getObject(ObjectColumn column);
    void setObject(ObjectColumn column);

    void deltaByte(ByteColumn column, byte delta);
    void deltaShort(ShortColumn column, short delta);
    void deltaInt(IntColumn column, int delta);
    void deltaLong(ByteColumn column, long delta);
    void deltaFloat(FloatColumn column, float delta);
    void deltaDouble(DoubleColumn column, double delta);


}
