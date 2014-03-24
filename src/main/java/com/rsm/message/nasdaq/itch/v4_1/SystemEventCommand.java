/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class SystemEventCommand
{
    public static final int BLOCK_LENGTH = 32;
    public static final int TEMPLATE_ID = 1083;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final SystemEventCommand parentMessage = this;
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

    public SystemEventCommand wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public SystemEventCommand wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
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

    public static int streamHeaderId()
    {
        return 1;
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
        streamHeader.wrap(buffer, offset + 0, actingVersion);
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
        return ITCHMessageType.get(CodecUtil.charGet(buffer, offset + 26));
    }

    public SystemEventCommand messageType(final ITCHMessageType value)
    {
        CodecUtil.charPut(buffer, offset + 26, value.value());
        return this;
    }

    public static int timestampId()
    {
        return 3;
    }

    public static String timestampMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "timestamp";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long timestampNullValue()
    {
        return 4294967294L;
    }

    public static long timestampMinValue()
    {
        return 0L;
    }

    public static long timestampMaxValue()
    {
        return 4294967293L;
    }

    public long timestamp()
    {
        return CodecUtil.uint32Get(buffer, offset + 27, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public SystemEventCommand timestamp(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 27, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int eventCodeId()
    {
        return 4;
    }

    public static String eventCodeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte eventCodeNullValue()
    {
        return (byte)0;
    }

    public static byte eventCodeMinValue()
    {
        return (byte)32;
    }

    public static byte eventCodeMaxValue()
    {
        return (byte)126;
    }

    public byte eventCode()
    {
        return CodecUtil.charGet(buffer, offset + 31);
    }

    public SystemEventCommand eventCode(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 31, value);
        return this;
    }
}
