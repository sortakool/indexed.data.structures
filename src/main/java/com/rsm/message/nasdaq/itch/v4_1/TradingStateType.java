/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum TradingStateType
{
    HALTED((byte)72),
    PAUSED((byte)80),
    QUOTATION((byte)81),
    TRADING((byte)84),
    NULL_VAL((byte)0);

    private final byte value;

    TradingStateType(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static TradingStateType get(final byte value)
    {
        switch (value)
        {
            case 72: return HALTED;
            case 80: return PAUSED;
            case 81: return QUOTATION;
            case 84: return TRADING;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
