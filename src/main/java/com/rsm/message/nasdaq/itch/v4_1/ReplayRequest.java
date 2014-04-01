/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class ReplayRequest
{
    public static final int BLOCK_LENGTH = 11;
    public static final int TEMPLATE_ID = 5000;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final ReplayRequest parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    private int actingBlockLength;
    private int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public int offset()
    {
        return offset;
    }

    public ReplayRequest wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public ReplayRequest wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public int size()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        buffer.checkLimit(limit);
        this.limit = limit;
    }

    public static int sessionId()
    {
        return 1;
    }

    public static String sessionMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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

    public byte session()
    {
        return CodecUtil.charGet(buffer, offset + 0);
    }

    public ReplayRequest session(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 0, value);
        return this;
    }

    public static int sequenceNumberId()
    {
        return 2;
    }

    public static String sequenceNumberMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long sequenceNumberNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long sequenceNumberMinValue()
    {
        return 0x0L;
    }

    public static long sequenceNumberMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long sequenceNumber()
    {
        return CodecUtil.uint64Get(buffer, offset + 1, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public ReplayRequest sequenceNumber(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 1, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int messageCountId()
    {
        return 3;
    }

    public static String messageCountMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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
        return CodecUtil.uint16Get(buffer, offset + 9, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public ReplayRequest messageCount(final int value)
    {
        CodecUtil.uint16Put(buffer, offset + 9, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }
}
