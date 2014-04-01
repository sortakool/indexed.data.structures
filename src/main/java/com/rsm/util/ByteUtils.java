package com.rsm.util;

import java.util.Arrays;

/**
 * @see java.nio.Bits
 * Created by Raymond on 12/4/13.
 */
public abstract class ByteUtils {

    public static final byte SPACE = (byte) ' ';
    public static final byte ZERO = (byte) 0;

    public static byte[] fillWithSpaces(byte[] byteArray) {
        Arrays.fill(byteArray, SPACE);
        return byteArray;
    }

    public static byte[] fillWithZeros(byte[] byteArray) {
        Arrays.fill(byteArray, ZERO);
        return byteArray;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Short                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#makeShort(byte, byte)
     * @param b1
     * @param b0
     * @return
     */
    public static short makeShort(byte b1, byte b0) {
        return (short)((b1 << 8) | (b0 & 0xff));
    }

    /**
     * @see java.nio.Bits#short1(short)
     * @param x
     * @return
     */
    public static byte short1(short x) { return (byte)(x >> 8); }

    /**
     * @see java.nio.Bits#short0(short)
     * @param x
     * @return
     */
    public static byte short0(short x) { return (byte)(x     ); }

//    public static void putShort(long a, short x, boolean bigEndian) {
//        if (bigEndian)
//            putShortB(a, x);
//        else
//            putShortL(a, x);
//    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Integer                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#makeInt(byte, byte, byte, byte)
     * @param b3
     * @param b2
     * @param b1
     * @param b0
     * @return
     */
    public static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    /**
     * @see java.nio.Bits#int3(int)
     * @param x
     * @return
     */
    public static byte int3(int x) { return (byte)(x >> 24); }
    /**
     * @see java.nio.Bits#int2(int)
     * @param x
     * @return
     */
    public static byte int2(int x) { return (byte)(x >> 16); }
    /**
     * @see java.nio.Bits#int1(int)
     * @param x
     * @return
     */
    public static byte int1(int x) { return (byte)(x >>  8); }
    /**
     * @see java.nio.Bits#int0(int)
     * @param x
     * @return
     */
    public static byte int0(int x) { return (byte)(x      ); }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Long                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#getLong(java.nio.ByteBuffer, int, boolean)
     * @param bytes
     * @param position
     * @param bigEndian
     * @return
     */
    public static long getLong(byte[] bytes, int position, boolean bigEndian) {
        return bigEndian ? getLongBigEndian(bytes, position) : getLongLittleEndian(bytes, position);
    }

    /**
     * @see java.nio.Bits#getLongB(java.nio.ByteBuffer, int)
     *
     * @param byteArray
     * @param position
     * @return
     */
    public static long getLongBigEndian(byte[] byteArray, int position) {
        return makeLong(byteArray[position],
                byteArray[position + 1],
                byteArray[position + 2],
                byteArray[position + 3],
                byteArray[position + 4],
                byteArray[position + 5],
                byteArray[position + 6],
                byteArray[position + 7]);
    }

    /**
     * @see java.nio.Bits#getLongL(java.nio.ByteBuffer, int)
     *
     * @param bytes
     * @param position
     * @return
     */
    public static long getLongLittleEndian(byte[] bytes, int position) {
        return makeLong(bytes[position + 7],
                bytes[position + 6],
                bytes[position + 5],
                bytes[position + 4],
                bytes[position + 3],
                bytes[position + 2],
                bytes[position + 1],
                bytes[position + 0]);
    }

    /**
     * @see java.nio.Bits#putLong(java.nio.ByteBuffer, int, long, boolean)
     *
     * @param bytes
     * @param position
     * @param x
     * @param bigEndian
     */
    public static void putLong(byte[] bytes, int position, long x, boolean bigEndian) {
        if (bigEndian)
            putLongBigEndian(bytes, position, x);
        else
            putLongLittleEndian(bytes, position, x);
    }

    /**
     * @see java.nio.Bits#putLongB(java.nio.ByteBuffer, int, long)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putLongBigEndian(byte[] bytes, int position, long x) {
        bytes[position    ] = long7(x);
        bytes[position + 1] = long6(x);
        bytes[position + 2] = long5(x);
        bytes[position + 3] = long4(x);
        bytes[position + 4] = long3(x);
        bytes[position + 5] = long2(x);
        bytes[position + 6] = long1(x);
        bytes[position + 7] = long0(x);
    }

    /**
     * @see java.nio.Bits#putLongB(java.nio.ByteBuffer, int, long)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putLongLittleEndian(byte[] bytes, int position, long x) {
        bytes[position + 7] = long7(x);
        bytes[position + 6] = long6(x);
        bytes[position + 5] = long5(x);
        bytes[position + 4] = long4(x);
        bytes[position + 3] = long3(x);
        bytes[position + 2] = long2(x);
        bytes[position + 1] = long1(x);
        bytes[position    ] = long0(x);
    }

    /**
     * @see java.nio.Bits#makeLong(byte, byte, byte, byte, byte, byte, byte, byte)
     *
     * @param b7
     * @param b6
     * @param b5
     * @param b4
     * @param b3
     * @param b2
     * @param b1
     * @param b0
     * @return
     */
    public static long makeLong(byte b7, byte b6, byte b5, byte b4,
                                 byte b3, byte b2, byte b1, byte b0)
    {
        return ((((long)b7       ) << 56) |
                (((long)b6 & 0xff) << 48) |
                (((long)b5 & 0xff) << 40) |
                (((long)b4 & 0xff) << 32) |
                (((long)b3 & 0xff) << 24) |
                (((long)b2 & 0xff) << 16) |
                (((long)b1 & 0xff) <<  8) |
                (((long)b0 & 0xff)      ));
    }

    /**
     * @see java.nio.Bits#long7(long)
     * @param x
     * @return
     */
    public static byte long7(long x) { return (byte)(x >> 56); }
    /**
     * @see java.nio.Bits#long6(long)
     * @param x
     * @return
     */
    public static byte long6(long x) { return (byte)(x >> 48); }
    /**
     * @see java.nio.Bits#long5(long)
     * @param x
     * @return
     */
    public static byte long5(long x) { return (byte)(x >> 40); }
    /**
     * @see java.nio.Bits#long4(long)
     * @param x
     * @return
     */
    public static byte long4(long x) { return (byte)(x >> 32); }
    /**
     * @see java.nio.Bits#long3(long)
     * @param x
     * @return
     */
    public static byte long3(long x) { return (byte)(x >> 24); }
    /**
     * @see java.nio.Bits#long2(long)
     * @param x
     * @return
     */
    public static byte long2(long x) { return (byte)(x >> 16); }
    /**
     * @see java.nio.Bits#long1(long)
     * @param x
     * @return
     */
    public static byte long1(long x) { return (byte)(x >>  8); }
    /**
     * @see java.nio.Bits#long0(long)
     * @param x
     * @return
     */
    public static byte long0(long x) { return (byte)(x      ); }
}
