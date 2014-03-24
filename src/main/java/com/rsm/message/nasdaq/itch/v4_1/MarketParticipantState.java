/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum MarketParticipantState
{
    ACTIVE((byte)65),
    EXCUSED_WITHDRAWN((byte)69),
    WITHDRAWN((byte)87),
    SUSPENDED((byte)83),
    DELETED((byte)68),
    NULL_VAL((byte)0);

    private final byte value;

    MarketParticipantState(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static MarketParticipantState get(final byte value)
    {
        switch (value)
        {
            case 65: return ACTIVE;
            case 69: return EXCUSED_WITHDRAWN;
            case 87: return WITHDRAWN;
            case 83: return SUSPENDED;
            case 68: return DELETED;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
