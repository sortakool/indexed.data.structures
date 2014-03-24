/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class StreamHeader
{
    private DirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public StreamHeader wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 26;
    }

    public static long timestampNanosNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long timestampNanosMinValue()
    {
        return 0x0L;
    }

    public static long timestampNanosMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long timestampNanos()
    {
        return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StreamHeader timestampNanos(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static byte majorNullValue()
    {
        return (byte)0;
    }

    public static byte majorMinValue()
    {
        return (byte)32;
    }

    public static byte majorMaxValue()
    {
        return (byte)126;
    }

    public byte major()
    {
        return CodecUtil.charGet(buffer, offset + 8);
    }

    public StreamHeader major(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 8, value);
        return this;
    }

    public static byte minorNullValue()
    {
        return (byte)0;
    }

    public static byte minorMinValue()
    {
        return (byte)32;
    }

    public static byte minorMaxValue()
    {
        return (byte)126;
    }

    public byte minor()
    {
        return CodecUtil.charGet(buffer, offset + 9);
    }

    public StreamHeader minor(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 9, value);
        return this;
    }

    public static long sourceNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long sourceMinValue()
    {
        return 0x0L;
    }

    public static long sourceMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long source()
    {
        return CodecUtil.uint64Get(buffer, offset + 10, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StreamHeader source(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 10, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static long idNullValue()
    {
        return 4294967294L;
    }

    public static long idMinValue()
    {
        return 0L;
    }

    public static long idMaxValue()
    {
        return 4294967293L;
    }

    public long id()
    {
        return CodecUtil.uint32Get(buffer, offset + 18, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StreamHeader id(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 18, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static long refNullValue()
    {
        return 4294967294L;
    }

    public static long refMinValue()
    {
        return 0L;
    }

    public static long refMaxValue()
    {
        return 4294967293L;
    }

    public long ref()
    {
        return CodecUtil.uint32Get(buffer, offset + 22, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StreamHeader ref(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 22, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }
}
