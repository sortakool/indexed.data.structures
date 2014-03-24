/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum ITCHMessageType
{
    TIMESTAMP_SECONDS((byte)84),
    SYSTEM_EVENT((byte)83),
    STOCK_DIRECTORY((byte)82),
    STOCK_TRADING_ACTION((byte)72),
    REG_SHO_RESTRICTION((byte)89),
    MARKET_PARTICIPANT_POSITION((byte)76),
    ADD_ORDER_NO_MPID_ATTRIBUTION((byte)65),
    ADD_ORDER_WITH_MPID_ATTRIBUTION((byte)70),
    ORDER_EXECUTED((byte)69),
    ORDER_EXECUTED_WITH_PRICE((byte)67),
    ORDER_CANCEL((byte)88),
    ORDER_DELETE((byte)68),
    ORDER_REPLACE((byte)85),
    TRADE_MESSAGE_NON_CROSS((byte)80),
    CROSS_TRADE((byte)81),
    BROKEN_TRADE((byte)66),
    NOII((byte)73),
    RETAIL_INTEREST((byte)78),
    NULL_VAL((byte)0);

    private final byte value;

    ITCHMessageType(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static ITCHMessageType get(final byte value)
    {
        switch (value)
        {
            case 84: return TIMESTAMP_SECONDS;
            case 83: return SYSTEM_EVENT;
            case 82: return STOCK_DIRECTORY;
            case 72: return STOCK_TRADING_ACTION;
            case 89: return REG_SHO_RESTRICTION;
            case 76: return MARKET_PARTICIPANT_POSITION;
            case 65: return ADD_ORDER_NO_MPID_ATTRIBUTION;
            case 70: return ADD_ORDER_WITH_MPID_ATTRIBUTION;
            case 69: return ORDER_EXECUTED;
            case 67: return ORDER_EXECUTED_WITH_PRICE;
            case 88: return ORDER_CANCEL;
            case 68: return ORDER_DELETE;
            case 85: return ORDER_REPLACE;
            case 80: return TRADE_MESSAGE_NON_CROSS;
            case 81: return CROSS_TRADE;
            case 66: return BROKEN_TRADE;
            case 73: return NOII;
            case 78: return RETAIL_INTEREST;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
