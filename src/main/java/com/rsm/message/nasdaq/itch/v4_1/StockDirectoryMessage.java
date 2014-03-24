/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public class StockDirectoryMessage
{
    public static final int BLOCK_LENGTH = 20;
    public static final int TEMPLATE_ID = 82;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 0;

    private final StockDirectoryMessage parentMessage = this;
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

    public StockDirectoryMessage wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public StockDirectoryMessage wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
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
        return ITCHMessageType.get(CodecUtil.charGet(buffer, offset + 0));
    }

    public StockDirectoryMessage messageType(final ITCHMessageType value)
    {
        CodecUtil.charPut(buffer, offset + 0, value.value());
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
        return CodecUtil.uint32Get(buffer, offset + 1, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StockDirectoryMessage nanoseconds(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 1, value, java.nio.ByteOrder.BIG_ENDIAN);
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

        return CodecUtil.charGet(buffer, this.offset + 5 + (index * 1));
    }

    public void stock(final int index, final byte value)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 5 + (index * 1), value);
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

        CodecUtil.charsGet(buffer, this.offset + 5, dst, dstOffset, length);
        return length;
    }

    public StockDirectoryMessage putStock(final byte[] src, final int srcOffset)
    {
        final int length = 8;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 5, src, srcOffset, length);
        return this;
    }

    public static int marketCategoryId()
    {
        return 7;
    }

    public static String marketCategoryMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte marketCategoryNullValue()
    {
        return (byte)0;
    }

    public static byte marketCategoryMinValue()
    {
        return (byte)32;
    }

    public static byte marketCategoryMaxValue()
    {
        return (byte)126;
    }

    public byte marketCategory()
    {
        return CodecUtil.charGet(buffer, offset + 13);
    }

    public StockDirectoryMessage marketCategory(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 13, value);
        return this;
    }

    public static int financialStatusIndicatorId()
    {
        return 8;
    }

    public static String financialStatusIndicatorMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte financialStatusIndicatorNullValue()
    {
        return (byte)0;
    }

    public static byte financialStatusIndicatorMinValue()
    {
        return (byte)32;
    }

    public static byte financialStatusIndicatorMaxValue()
    {
        return (byte)126;
    }

    public byte financialStatusIndicator()
    {
        return CodecUtil.charGet(buffer, offset + 14);
    }

    public StockDirectoryMessage financialStatusIndicator(final byte value)
    {
        CodecUtil.charPut(buffer, offset + 14, value);
        return this;
    }

    public static int roundLotSizeId()
    {
        return 9;
    }

    public static String roundLotSizeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long roundLotSizeNullValue()
    {
        return 4294967294L;
    }

    public static long roundLotSizeMinValue()
    {
        return 0L;
    }

    public static long roundLotSizeMaxValue()
    {
        return 4294967293L;
    }

    public long roundLotSize()
    {
        return CodecUtil.uint32Get(buffer, offset + 15, java.nio.ByteOrder.BIG_ENDIAN);
    }

    public StockDirectoryMessage roundLotSize(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 15, value, java.nio.ByteOrder.BIG_ENDIAN);
        return this;
    }

    public static int roundLotsOnlyId()
    {
        return 10;
    }

    public static String roundLotsOnlyMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public YesNoType roundLotsOnly()
    {
        return YesNoType.get(CodecUtil.charGet(buffer, offset + 19));
    }

    public StockDirectoryMessage roundLotsOnly(final YesNoType value)
    {
        CodecUtil.charPut(buffer, offset + 19, value.value());
        return this;
    }
}
