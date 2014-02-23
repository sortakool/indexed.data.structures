package com.rsm.util;

/**
 * @see java.nio.Bits
 * Created by Raymond on 12/4/13.
 */
public abstract class ByteUtils {

    public static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    public static byte int3(int x) { return (byte)(x >> 24); }
    public static byte int2(int x) { return (byte)(x >> 16); }
    public static byte int1(int x) { return (byte)(x >>  8); }
    public static byte int0(int x) { return (byte)(x      ); }
}
