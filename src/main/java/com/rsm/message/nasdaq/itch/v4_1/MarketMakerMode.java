/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum MarketMakerMode
{
    NORMAL((byte)78),
    PASSIVE((byte)80),
    SYNDICATE((byte)83),
    PRE_SYNDICATE((byte)82),
    PENALTY((byte)76),
    NULL_VAL((byte)0);

    private final byte value;

    MarketMakerMode(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static MarketMakerMode get(final byte value)
    {
        switch (value)
        {
            case 78: return NORMAL;
            case 80: return PASSIVE;
            case 83: return SYNDICATE;
            case 82: return PRE_SYNDICATE;
            case 76: return PENALTY;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
