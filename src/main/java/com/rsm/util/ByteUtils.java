package com.rsm.util;

import sun.misc.Unsafe;
import uk.co.real_logic.sbe.util.BitUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * @see java.nio.Bits
 * Created by Raymond on 12/4/13.
 */
public abstract class ByteUtils {

    public static final byte SPACE = (byte) ' ';
    public static final byte ZERO = (byte) 0;

    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
    public static final Unsafe UNSAFE = BitUtil.getUnsafe();
    public static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    public static byte[] fillWithSpaces(byte[] byteArray) {
        Arrays.fill(byteArray, SPACE);
        return byteArray;
    }

    public static byte[] fillWithZeros(byte[] byteArray) {
        Arrays.fill(byteArray, ZERO);
        return byteArray;
    }

    /**
     * @see uk.co.real_logic.sbe.codec.java.DirectBuffer#wrap(byte[])
     * @param buffer
     * @return
     */
    public static long getAddressOffset(final byte[] buffer) {
        return BYTE_ARRAY_OFFSET;
    }

    /**
     * @see uk.co.real_logic.sbe.codec.java.DirectBuffer#wrap(java.nio.ByteBuffer)
     * @param buffer
     * @return
     */
    public static long getAddressOffset(final ByteBuffer buffer) {
        long addressOffset;
        if (buffer.hasArray())
        {
            addressOffset = BYTE_ARRAY_OFFSET + buffer.arrayOffset();
        }
        else
        {
            addressOffset = ((sun.nio.ch.DirectBuffer)buffer).address();
        }
        return addressOffset;
    }

    public static byte[] getByteArray(final ByteBuffer buffer) {
        if (buffer.hasArray())
        {
            return buffer.array();
        }
        else
        {
            return null;
        }
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Short                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#getShort(java.nio.ByteBuffer, int, boolean)
     * @param bytes
     * @param position
     * @param bigEndian
     * @return
     */
    public static short getShort(byte[] bytes, int position, boolean bigEndian) {
        return bigEndian ? getShortBigEndian(bytes, position) : getShortLittleEndian(bytes, position);
    }

    /**
     * @see java.nio.Bits#getShortB(java.nio.ByteBuffer, int)
     * @param bytes
     * @param position
     * @return
     */
    public static short getShortBigEndian(byte[] bytes, int position) {
        return makeShort(   bytes[position],
                            bytes[position + 1] );
    }

    /**
     * @see java.nio.Bits#getShortL(java.nio.ByteBuffer, int)
     * @param bytes
     * @param position
     * @return
     */
    public static short getShortLittleEndian(byte[] bytes, int position) {
        return makeShort(   bytes[position + 1],
                            bytes[position] );
    }

    /**
     * @see java.nio.Bits#putShort(java.nio.ByteBuffer, int, short, boolean)
     *
     * @param bytes
     * @param position
     * @param x
     * @param bigEndian
     */
    public static void putShort(byte[] bytes, int position, short x, boolean bigEndian) {
        if (bigEndian)
            putShortBigEndian(bytes, position, x);
        else
            putShortLittleEndian(bytes, position, x);
    }

    /**
     * @see java.nio.Bits#putShortB(java.nio.ByteBuffer, int, short)
     *
     * @param bytes
     * @param bi
     * @param x
     */
    public static void putShortBigEndian(byte[] bytes, int bi, short x) {
        bytes[bi + 1] = short0(x) ;
        bytes[bi    ] = short1(x);
    }

    /**
     * @see java.nio.Bits#putShortL(java.nio.ByteBuffer, int, short)
     *
     * @param bytes
     * @param bi
     * @param x
     */
    public static void putShortLittleEndian(byte[] bytes, int bi, short x) {
        bytes[bi    ] = short0(x) ;
        bytes[bi + 1] = short1(x);
    }

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

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Integer                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * java.nio.Bits#getInt(java.nio.ByteBuffer, int, boolean)
     *
     * @param bytes
     * @param position
     * @param bigEndian
     * @return
     */
    public static int getInt(byte[] bytes, int position, boolean bigEndian) {
        return bigEndian ? getIntBigEndian(bytes, position) : getIntLittleEndian(bytes, position) ;
    }

    /**
     * @see java.nio.Bits#getIntB(java.nio.ByteBuffer, int)
     *
     * @param bytes
     * @param position
     * @return
     */
    public static int getIntBigEndian(byte[] bytes, int position) {
        return makeInt( bytes[position    ],
                        bytes[position + 1],
                        bytes[position + 2],
                        bytes[position + 3]
                );
    }

    /**
     * @see java.nio.Bits#getIntL(java.nio.ByteBuffer, int)
     *
     * @param bytes
     * @param position
     * @return
     */
    public static int getIntLittleEndian(byte[] bytes, int position) {
        return makeInt( bytes[position + 3],
                        bytes[position + 2],
                        bytes[position + 1],
                        bytes[position    ]
                );
    }

    /**
     * @see java.nio.Bits#getInt(java.nio.ByteBuffer, int, boolean)
     * @see uk.co.real_logic.sbe.codec.java.DirectBuffer#getInt(int, java.nio.ByteOrder)
     *
     * @param bb
     * @param bi
     * @param bigEndian
     * @return
     */
    public static int getInt(ByteBuffer bb, int bi, boolean bigEndian) {
        return bigEndian ? getIntB(bb, bi) : getIntL(bb, bi) ;
    }

    /**
     * @see java.nio.Bits#getInt(java.nio.ByteBuffer, int, boolean)
     * @see uk.co.real_logic.sbe.codec.java.DirectBuffer#getInt(int, java.nio.ByteOrder)
     *
     * @param bb
     * @param index
     * @return
     */
    public static int getIntB(ByteBuffer bb, int index) {
        return getInt(bb, index, ByteOrder.BIG_ENDIAN);
    }

    public static int getIntL(ByteBuffer bb, int index) {
        return getInt(bb, index, ByteOrder.LITTLE_ENDIAN);
    }

    public static int getInt(ByteBuffer bb, int index, ByteOrder byteOrder) {
        final long addressOffset = getAddressOffset(bb);
        final byte[] byteArray = getByteArray(bb);
        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder)
        {
            bits = Integer.reverseBytes(bits);
        }
        return bits;
    }

    /**
     * @see java.nio.Bits#putInt(java.nio.ByteBuffer, int, int, boolean)
     * @param bytes
     * @param position
     * @param x
     * @param bigEndian
     */
    public static void putInt(byte[] bytes, int position, int x, boolean bigEndian) {
        if (bigEndian)
            putIntBigEndian(bytes, position, x);
        else
            putIntLittleEndian(bytes, position, x);
    }

    /**
     * @see java.nio.Bits#putIntB(java.nio.ByteBuffer, int, int)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putIntBigEndian(byte[] bytes, int position, int x) {
        bytes[position    ] = int3(x);
        bytes[position + 1] = int2(x);
        bytes[position + 2] = int1(x);
        bytes[position + 3] = int0(x);
    }

    /**
     * @see java.nio.Bits#putIntL(java.nio.ByteBuffer, int, int)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putIntLittleEndian(byte[] bytes, int position, int x) {
        bytes[position + 3] = int3(x);
        bytes[position + 2] = int2(x);
        bytes[position + 1] = int1(x);
        bytes[position    ] = int0(x);
    }

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

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Float                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#getFloat(java.nio.ByteBuffer, int, boolean)
     *
     * @param bytes
     * @param position
     * @param bigEndian
     * @return
     */
    public static float getFloat(byte[] bytes, int position, boolean bigEndian) {
        return bigEndian ? getFloatBigEndian(bytes, position) : getFloatLittleEndian(bytes, position);
    }

    /**
     * @see java.nio.Bits#getFloatL(java.nio.ByteBuffer, int)
     * @param bytes
     * @param position
     * @return
     */
    public static float getFloatBigEndian(byte[] bytes, int position) {
        return Float.intBitsToFloat(getIntBigEndian(bytes, position));
    }

    /**
     * @see java.nio.Bits#getFloatL(java.nio.ByteBuffer, int)
     * @param bytes
     * @param position
     * @return
     */
    public static float getFloatLittleEndian(byte[] bytes, int position) {
        return Float.intBitsToFloat(getIntLittleEndian(bytes, position));
    }

    /**
     * @see java.nio.Bits#getFloat(java.nio.ByteBuffer, int, boolean)
     * @param bb
     * @param bi
     * @param bigEndian
     * @return
     */
    public static float getFloat(ByteBuffer bb, int bi, boolean bigEndian) {
        return bigEndian ? getFloatB(bb, bi) : getFloatL(bb, bi);
    }

    public static float getFloatB(ByteBuffer bb, int bi) {
        return Float.intBitsToFloat(getIntB(bb, bi));
    }

    public static float getFloatL(ByteBuffer bb, int bi) {
        return Float.intBitsToFloat(getIntL(bb, bi));
    }

    /**
     * @see java.nio.Bits#putFloat(java.nio.ByteBuffer, int, float, boolean)
     *
     * @param bytes
     * @param position
     * @param x
     * @param bigEndian
     */
    public static void putFloat(byte[] bytes, int position, float x, boolean bigEndian) {
        if (bigEndian)
            putFloatBigEndian(bytes, position, x);
        else
            putFloatLittleEndian(bytes, position, x);
    }

    /**
     * @see java.nio.Bits#putFloatB(java.nio.ByteBuffer, int, float)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putFloatBigEndian(byte[] bytes, int position, float x) {
        putIntBigEndian(bytes, position, Float.floatToRawIntBits(x));
    }

    /**
     * @see java.nio.Bits#putFloatL(java.nio.ByteBuffer, int, float)
     *
     * @param bytes
     * @param position
     * @param x
     */
    public static void putFloatLittleEndian(byte[] bytes, int position, float x) {
        putIntLittleEndian(bytes, position, Float.floatToRawIntBits(x));
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Char                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * @see java.nio.Bits#char1(char)
     * @param x
     * @return
     */
    public static byte char1(char x) { return (byte)(x >> 8); }

    /**
     * @see java.nio.Bits#char0(char)
     * @param x
     * @return
     */
    public static byte char0(char x) { return (byte)(x     ); }
}
