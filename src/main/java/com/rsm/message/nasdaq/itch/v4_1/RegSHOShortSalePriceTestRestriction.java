/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.itch.v4_1;

import uk.co.real_logic.sbe.codec.java.*;

public enum RegSHOShortSalePriceTestRestriction
{
    NO_PRICE_TEST_IN_PLACE((byte)48),
    IN_EFFECT_DUE_TO_AN_INTRADAY_PRICE_DROP_IN_SECURITY((byte)49),
    QUOTATION((byte)50),
    TEST_RESTRICTION_REMAINS_IN_EFFECT((byte)84),
    NULL_VAL((byte)0);

    private final byte value;

    RegSHOShortSalePriceTestRestriction(final byte value)
    {
        this.value = value;
    }

    public byte value()
    {
        return value;
    }

    public static RegSHOShortSalePriceTestRestriction get(final byte value)
    {
        switch (value)
        {
            case 48: return NO_PRICE_TEST_IN_PLACE;
            case 49: return IN_EFFECT_DUE_TO_AN_INTRADAY_PRICE_DROP_IN_SECURITY;
            case 50: return QUOTATION;
            case 84: return TEST_RESTRICTION_REMAINS_IN_EFFECT;
        }

        if ((byte)0 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
