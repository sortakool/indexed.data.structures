/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum YesNoType
{
    YES((byte)89),
    NO((byte)78),
    NULL_VAL((byte)0);

    private final byte value;

    YesNoType(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static YesNoType get(final byte value)
    {
        switch (value)
        {
            case 89: return YES;
            case 78: return NO;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
