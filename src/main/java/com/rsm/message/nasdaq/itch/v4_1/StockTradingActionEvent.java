/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class StockTradingActionEvent
{
    public static final int BLOCK_LENGTH = 53;
    public static final int TEMPLATE_ID = 2072;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final StockTradingActionEvent parentMessage = this;
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

    public StockTradingActionEvent wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public StockTradingActionEvent wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
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

    public StockTradingActionEvent sequence(final long value)
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

    public StockTradingActionEvent messageType(final ITCHMessageType value)
    {
        CodecUtil.charPut(buffer, offset + 34, value.value());
        return this;
    }

    public static int nanosecondsId()
    {
        return 5;
    }

    public static String nanosecondsMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "timestamp";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long nanosecondsNullValue()
    {
        return 4294967294L;
    }

    public static long nanosecondsMinValue()
    {
        return 0L;
    }

    public static long nanosecondsMaxValue()
    {
        return 4294967293L;
    }

    public long nanoseconds()
    {
        return CodecUtil.uint32Get(buffer, offset + 35, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StockTradingActionEvent nanoseconds(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 35, value, java.nio.ByteOrder.BIG_ENDIAN);
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

        return CodecUtil.charGet(buffer, this.offset + 39 + (index * 1));
    }

    public void stock(final int index, final byte value)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 39 + (index * 1), value);
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

        CodecUtil.charsGet(buffer, this.offset + 39, dst, dstOffset, length);
        return length;
    }

    public StockTradingActionEvent putStock(final byte[] src, final int srcOffset)
    {
        final int length = 8;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 39, src, srcOffset, length);
        return this;
    }

    public static int tradingStateId()
    {
        return 11;
    }

    public static String tradingStateMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public TradingStateType tradingState()
    {
        return TradingStateType.get(CodecUtil.charGet(buffer, offset + 47));
    }

    public StockTradingActionEvent tradingState(final TradingStateType value)
    {
        CodecUtil.charPut(buffer, offset + 47, value.value());
        return this;
    }

    public static int reservedId()
    {
        return 12;
    }

    public static String reservedMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte reservedNullValue()
    {
        return (byte)0;
    }

    public static byte reservedMinValue()
    {
        return (byte)32;
    }

    public static byte reservedMaxValue()
    {
        return (byte)126;
    }

    public byte reserved()
    {
        return CodecUtil.charGet(buffer, offset + 48);
    }

    public StockTradingActionEvent reserved(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 48, value);
        return this;
    }

    public static int reasonId()
    {
        return 13;
    }

    public static String reasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte reasonNullValue()
    {
        return (byte)0;
    }

    public static byte reasonMinValue()
    {
        return (byte)32;
    }

    public static byte reasonMaxValue()
    {
        return (byte)126;
    }

    public static int reasonLength()
    {
        return 4;
    }

    public byte reason(final int index)
    {
        if (index < 0 || index >= 4)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.charGet(buffer, this.offset + 49 + (index * 1));
    }

    public void reason(final int index, final byte value)
    {
        if (index < 0 || index >= 4)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 49 + (index * 1), value);
    }

    public static String reasonCharacterEncoding()
    {
        return "ASCII";
    }

    public int getReason(final byte[] dst, final int dstOffset)
    {
        final int length = 4;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
        }

        CodecUtil.charsGet(buffer, this.offset + 49, dst, dstOffset, length);
        return length;
    }

    public StockTradingActionEvent putReason(final byte[] src, final int srcOffset)
    {
        final int length = 4;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 49, src, srcOffset, length);
        return this;
    }
}
