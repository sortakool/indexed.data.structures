/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class MarketParticipantPositionCommand
{
    public static final int BLOCK_LENGTH = 46;
    public static final int TEMPLATE_ID = 1076;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final MarketParticipantPositionCommand parentMessage = this;
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

    public MarketParticipantPositionCommand wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public MarketParticipantPositionCommand wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
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

    public MarketParticipantPositionCommand messageType(final ITCHMessageType value)
    {
        CodecUtil.charPut(buffer, offset + 26, value.value());
        return this;
    }

    public static int timestampNanosecondsId()
    {
        return 5;
    }

    public static String timestampNanosecondsMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "timestamp";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long timestampNanosecondsNullValue()
    {
        return 4294967294L;
    }

    public static long timestampNanosecondsMinValue()
    {
        return 0L;
    }

    public static long timestampNanosecondsMaxValue()
    {
        return 4294967293L;
    }

    public long timestampNanoseconds()
    {
        return CodecUtil.uint32Get(buffer, offset + 27, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public MarketParticipantPositionCommand timestampNanoseconds(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 27, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int mpidId()
    {
        return 15;
    }

    public static String mpidMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte mpidNullValue()
    {
        return (byte)0;
    }

    public static byte mpidMinValue()
    {
        return (byte)32;
    }

    public static byte mpidMaxValue()
    {
        return (byte)126;
    }

    public static int mpidLength()
    {
        return 4;
    }

    public byte mpid(final int index)
    {
        if (index < 0 || index >= 4)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.charGet(buffer, this.offset + 31 + (index * 1));
    }

    public void mpid(final int index, final byte value)
    {
        if (index < 0 || index >= 4)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 31 + (index * 1), value);
    }

    public static String mpidCharacterEncoding()
    {
        return "ASCII";
    }

    public int getMpid(final byte[] dst, final int dstOffset)
    {
        final int length = 4;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
        }

        CodecUtil.charsGet(buffer, this.offset + 31, dst, dstOffset, length);
        return length;
    }

    public MarketParticipantPositionCommand putMpid(final byte[] src, final int srcOffset)
    {
        final int length = 4;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 31, src, srcOffset, length);
        return this;
    }

    public static int stockId()
    {
        return 6;
    }

    public static String stockMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte stockNullValue()
    {
        return (byte)0;
    }

    public static byte stockMinValue()
    {
        return (byte)32;
    }

    public static byte stockMaxValue()
    {
        return (byte)126;
    }

    public static int stockLength()
    {
        return 8;
    }

    public byte stock(final int index)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.charGet(buffer, this.offset + 35 + (index * 1));
    }

    public void stock(final int index, final byte value)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 35 + (index * 1), value);
    }

    public static String stockCharacterEncoding()
    {
        return "ASCII";
    }

    public int getStock(final byte[] dst, final int dstOffset)
    {
        final int length = 8;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
        }

        CodecUtil.charsGet(buffer, this.offset + 35, dst, dstOffset, length);
        return length;
    }

    public MarketParticipantPositionCommand putStock(final byte[] src, final int srcOffset)
    {
        final int length = 8;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 35, src, srcOffset, length);
        return this;
    }

    public static int primaryMarketMakerId()
    {
        return 16;
    }

    public static String primaryMarketMakerMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public YesNoType primaryMarketMaker()
    {
        return YesNoType.get(CodecUtil.charGet(buffer, offset + 43));
    }

    public MarketParticipantPositionCommand primaryMarketMaker(final YesNoType value)
    {
        CodecUtil.charPut(buffer, offset + 43, value.value());
        return this;
    }

    public static int marketMakerModeId()
    {
        return 17;
    }

    public static String marketMakerModeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public MarketMakerMode marketMakerMode()
    {
        return MarketMakerMode.get(CodecUtil.charGet(buffer, offset + 44));
    }

    public MarketParticipantPositionCommand marketMakerMode(final MarketMakerMode value)
    {
        CodecUtil.charPut(buffer, offset + 44, value.value());
        return this;
    }

    public static int marketParticipantStateId()
    {
        return 18;
    }

    public static String marketParticipantStateMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public MarketParticipantState marketParticipantState()
    {
        return MarketParticipantState.get(CodecUtil.charGet(buffer, offset + 45));
    }

    public MarketParticipantPositionCommand marketParticipantState(final MarketParticipantState value)
    {
        CodecUtil.charPut(buffer, offset + 45, value.value());
        return this;
    }
}
