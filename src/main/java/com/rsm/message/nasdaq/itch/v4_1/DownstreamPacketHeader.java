/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class DownstreamPacketHeader
{
    private DirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public DownstreamPacketHeader wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 20;
    }

    public static byte sessionNullValue()
    {
        return (byte)0;
    }

    public static byte sessionMinValue()
    {
        return (byte)32;
    }

    public static byte sessionMaxValue()
    {
        return (byte)126;
    }

    public static int sessionLength()
    {
        return 10;
    }

    public byte session(final int index)
    {
        if (index < 0 || index >= 10)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.charGet(buffer, this.offset + 0 + (index * 1));
    }

    public void session(final int index, final byte value)
    {
        if (index < 0 || index >= 10)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 0 + (index * 1), value);
    }

    public static String sessionCharacterEncoding()
    {
        return "ASCII";
    }

    public int getSession(final byte[] dst, final int dstOffset)
    {
        final int length = 10;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
        }

        CodecUtil.charsGet(buffer, this.offset + 0, dst, dstOffset, length);
        return length;
    }

    public DownstreamPacketHeader putSession(final byte[] src, final int srcOffset)
    {
        final int length = 10;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 0, src, srcOffset, length);
        return this;
    }

    public static long sourceSequenceNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long sourceSequenceMinValue()
    {
        return 0x0L;
    }

    public static long sourceSequenceMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long sourceSequence()
    {
        return CodecUtil.uint64Get(buffer, offset + 10, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public DownstreamPacketHeader sourceSequence(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 10, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int messageCountNullValue()
    {
        return 65535;
    }

    public static int messageCountMinValue()
    {
        return 0;
    }

    public static int messageCountMaxValue()
    {
        return 65534;
    }

    public int messageCount()
    {
        return CodecUtil.uint16Get(buffer, offset + 18, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public DownstreamPacketHeader messageCount(final int value)
    {
        CodecUtil.uint16Put(buffer, offset + 18, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }
}
