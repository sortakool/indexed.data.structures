/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class TimestampSecondsEvent
{
    public static final int BLOCK_LENGTH = 39;
    public static final int TEMPLATE_ID = 2084;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final TimestampSecondsEvent parentMessage = this;
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

    public TimestampSecondsEvent wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public TimestampSecondsEvent wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
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

    public static int sequenceId()
    {
        return 1;
    }

    public static String sequenceMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long sequenceNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long sequenceMinValue()
    {
        return 0x0L;
    }

    public static long sequenceMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long sequence()
    {
        return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public TimestampSecondsEvent sequence(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int streamHeaderId()
    {
        return 2;
    }

    public static String streamHeaderMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    private final StreamHeader streamHeader = new StreamHeader();

    public StreamHeader streamHeader()
    {
        streamHeader.wrap(buffer, offset + 8, actingVersion);
        return streamHeader;
    }

    public static int messageTypeId()
    {
        return 1;
    }

    public static String messageTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public ITCHMessageType messageType()
    {
        return ITCHMessageType.get(CodecUtil.charGet(buffer, offset + 34));
    }

    public TimestampSecondsEvent messageType(final ITCHMessageType value)
    {
        CodecUtil.charPut(buffer, offset + 34, value.value());
        return this;
    }

    public static int secondsId()
    {
        return 2;
    }

    public static String secondsMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "seconds";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long secondsNullValue()
    {
        return 4294967294L;
    }

    public static long secondsMinValue()
    {
        return 0L;
    }

    public static long secondsMaxValue()
    {
        return 4294967293L;
    }

    public long seconds()
    {
        return CodecUtil.uint32Get(buffer, offset + 35, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public TimestampSecondsEvent seconds(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 35, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }
}
