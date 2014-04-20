package com.rsm.buffer;

import com.rsm.util.ByteUtils;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @see net.openhft.lang.io.MappedMemory
 * @see uk.co.real_logic.sbe.codec.java.DirectBuffer
 *
 * Created by rmanaloto on 4/14/14.
 */
public class NativeMappedMemory {

//    public static final Unsafe UNSAFE = BitUtil.getUnsafe();
//    public static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private byte[] byteArray;
    private MappedByteBuffer buffer;
    private long addressOffset;
    private long capacityAddressOffset;

    private ByteBuffer slice;

    private boolean mapped = false;

    // Invariants: mark <= position <= limit <= capacity
    private long mark = -1;
    private long position = 0;
    private long limit;
    private long capacity;

    public static NativeMappedMemory create(long capacity) throws IOException {
        NativeMappedMemory nativeMappedMemory = new NativeMappedMemory();
        nativeMappedMemory.capacity = capacity;
        nativeMappedMemory.mapped = false;
        return nativeMappedMemory;
    }

    private NativeMappedMemory()
    {
        this.mapped = false;
    }

    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory(final byte[] buffer)
    {
        wrap(buffer);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory(final MappedByteBuffer buffer)
    {
        wrap(buffer);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory(final ByteBuffer buffer)
    {
        wrap(buffer);
    }

    /**
     * Attach a view to an off-heap memory region by address.
     *
     * @param address where the memory begins off-heap
     * @param capacity of the buffer from the given address
     */
    public NativeMappedMemory(final long address, final int capacity)
    {
        wrap(address, capacity);
    }


    /**
     * Attach a view to a byte[] for providing direct access.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory wrap(final byte[] buffer)
    {
        addressOffset = ByteUtils.BYTE_ARRAY_OFFSET;
        capacity = buffer.length;
        capacityAddressOffset = addressOffset + capacity;
        byteArray = buffer;
        this.buffer = null;

        prepareSlice();

        mapped = true;
        clear();
        return this;
    }

    private void prepareSlice() {
        this.slice = ByteBuffer.allocateDirect(0);
        BitUtil.resetAddressAndCapacity(this.slice, addressOffset, (int) capacity);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory wrap(final MappedByteBuffer buffer)
    {
        this.buffer = buffer;
        return wrap((ByteBuffer)buffer);
    }

    /**
     * Attach a view to a {@link ByteBuffer} for providing direct access, the {@link ByteBuffer} can be
     * heap based or direct.
     *
     * @param buffer to which the view is attached.
     */
    public NativeMappedMemory wrap(final ByteBuffer buffer)
    {
        if (buffer.hasArray())
        {
            byteArray = buffer.array();
            addressOffset = ByteUtils.BYTE_ARRAY_OFFSET + buffer.arrayOffset();
        }
        else
        {
            byteArray = null;
            addressOffset = ((sun.nio.ch.DirectBuffer)buffer).address();
        }

        capacity = buffer.capacity();
        capacityAddressOffset = addressOffset + capacity;

        prepareSlice();

        mapped = true;
        clear();
        return this;
    }

    /**
     * Attach a view to an off-heap memory region by address.
     *
     * @param address where the memory begins off-heap
     * @param capacity of the buffer from the given address
     */
    public NativeMappedMemory wrap(final long address, final int capacity)
    {
        addressOffset = address;
        this.capacity = capacity;
        capacityAddressOffset = addressOffset + capacity;
        byteArray = null;
        buffer = null;

        prepareSlice();

        mapped = true;
        clear();
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[position=").append(position).append(" ")
          .append("limit=").append(limit).append(" ")
          .append("capacity=").append(capacity).append(" ")
          .append("addressOffset=").append(addressOffset).append(" ")
          .append("mapped=").append(mapped).append(" ")
          .append("]")
        ;
        return sb.toString();
    }

    /**
     * Returns the current hash code of this buffer.
     *
     * <p> The hash code of a NativeMappedMemory depends only upon its remaining
     * elements; that is, upon the elements from <tt>position()</tt> up to, and
     * including, the element at <tt>limit()</tt>&nbsp;-&nbsp;<tt>1</tt>.
     *
     * <p> Because NativeMappedMemory hash codes are content-dependent, it is inadvisable
     * to use NativeMappedMemory as keys in hash maps or similar data structures unless it
     * is known that their contents will not change.  </p>
     *
     * @return  The current hash code of this buffer
     */
    public int hashCode() {
        int h = 1;
        long p = position();
        for (long i = limit() - 1; i >= p; i--) {
            h = 31 * h + (int) get(i);
        }
        return h;
    }

    /**
     * Tells whether or not this buffer is equal to another object.
     *
     * <p> Two byte buffers are equal if, and only if,
     *
     * <ol>
     *
     *   <li><p> They have the same element type,  </p></li>
     *
     *   <li><p> They have the same number of remaining elements, and
     *   </p></li>
     *
     *   <li><p> The two sequences of remaining elements, considered
     *   independently of their starting positions, are pointwise equal.
     *   </p></li>
     *
     * </ol>
     *
     * <p> A byte buffer is not equal to any other type of object.  </p>
     *
     * @param  ob  The object to which this buffer is to be compared
     *
     * @return  <tt>true</tt> if, and only if, this buffer is equal to the
     *           given object
     */
    public boolean equals(Object ob) {
        if (this == ob)
            return true;
        if (!(ob instanceof NativeMappedMemory)) {
            return false;
        }
        NativeMappedMemory that = (NativeMappedMemory)ob;
        if (this.remaining() != that.remaining()) {
            return false;
        }
        if(this.isMapped() != that.isMapped()) {
            return false;
        }
        long p = this.position();
        for (long i = this.limit() - 1, j = that.limit() - 1; i >= p; i--, j--) {
            if (!equals(this.get(i), that.get(j))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equals(byte x, byte y) {
        return x == y;
    }

    public long addressOffset() {
        return addressOffset;
    }

    public long capacityAddressOffset() {
        return capacityAddressOffset;
    }

    public ByteBuffer slice() {
        slice.clear();
        return slice;
    }

    public boolean isMapped() {
        return mapped;
    }

    public MappedByteBuffer buffer() {
        return buffer;
    }

    public byte[] byteArray() {
        return byteArray;
    }

    public void force() {
        buffer.force();
    }

    public boolean reserve() {
        boolean reserved = false;
        if(!mapped) {
            mapped = true;
            reserved = true;
        }
        return  reserved;
    }

    public boolean release() {
        boolean released = false;
        if(mapped) {
            close();
            released = true;
        }
        return released;
    }

    /**
     * Check that a given limit is not greater than the capacity of a buffer from a given offset.
     *
     * Can be overridden in a DirectBuffer subclass to enable an extensible buffer or handle retry after a flush.
     *
     * @param limit access is required to.
     * @throws IndexOutOfBoundsException if limit is beyond buffer capacity.
     */
    public void checkLimit(final long limit)
    {
        if (limit > capacity)
        {
            final String msg = String.format("limit=%d is beyond capacity=%d",
                    Long.valueOf(limit),
                    Long.valueOf(capacity));

            throw new IndexOutOfBoundsException(msg);
        }
    }

    /**
     * Returns this buffer's capacity.
     *
     * @return  The capacity of this buffer
     */
    public final long capacity() {
        return capacity;
    }

    /**
     * Returns this buffer's position.
     *
     * @return  The position of this buffer
     */
    public final long position() {
        return position;
    }

    /**
     * Sets this buffer's position.  If the mark is defined and larger than the
     * new position then it is discarded.
     *
     * @param  newPosition
     *         The new position value; must be non-negative
     *         and no larger than the current limit
     *
     * @return  This buffer
     *
     * @throws  IllegalArgumentException
     *          If the preconditions on <tt>newPosition</tt> do not hold
     */
    public final NativeMappedMemory position(long newPosition) {
        if ((newPosition > limit) || (newPosition < 0))
            throw new IllegalArgumentException();
        position = newPosition;
        if (mark > position) mark = -1;
        return this;
    }

    /**
     * Returns this buffer's limit.
     *
     * @return  The limit of this buffer
     */
    public final long limit() {
        return limit;
    }

    /**
     * Sets this buffer's limit.  If the position is larger than the new limit
     * then it is set to the new limit.  If the mark is defined and larger than
     * the new limit then it is discarded.
     *
     * @param  newLimit
     *         The new limit value; must be non-negative
     *         and no larger than this buffer's capacity
     *
     * @return  This buffer
     *
     * @throws  IllegalArgumentException
     *          If the preconditions on <tt>newLimit</tt> do not hold
     */
    public final NativeMappedMemory limit(long newLimit) {
        if ((newLimit > capacity) || (newLimit < 0))
            throw new IllegalArgumentException();
        limit = newLimit;
        if (position > limit) position = limit;
        if (mark > limit) mark = -1;
        return this;
    }

    /**
     * Sets this buffer's mark at its position.
     *
     * @return  This buffer
     */
    public final NativeMappedMemory mark() {
        mark = position;
        return this;
    }

    /**
     * Resets this buffer's position to the previously-marked position.
     *
     * <p> Invoking this method neither changes nor discards the mark's
     * value. </p>
     *
     * @return  This buffer
     *
     * @throws java.nio.InvalidMarkException
     *          If the mark has not been set
     */
    public final NativeMappedMemory reset() {
        long m = mark;
        if (m < 0)
            throw new InvalidMarkException();
        position = m;
        return this;
    }

    /**
     * Clears this buffer.  The position is set to zero, the limit is set to
     * the capacity, and the mark is discarded.
     *
     * <p> Invoke this method before using a sequence of channel-read or
     * <i>put</i> operations to fill this buffer.  For example:
     *
     * <blockquote><pre>
     * buf.clear();     // Prepare buffer for reading
     * in.read(buf);    // Read data</pre></blockquote>
     *
     * <p> This method does not actually erase the data in the buffer, but it
     * is named as if it did because it will most often be used in situations
     * in which that might as well be the case. </p>
     *
     * @return  This buffer
     */
    public final NativeMappedMemory clear() {
        position = 0;
        limit = capacity;
        mark = -1;
        return this;
    }

    /**
     * Flips this buffer.  The limit is set to the current position and then
     * the position is set to zero.  If the mark is defined then it is
     * discarded.
     *
     * <p> After a sequence of channel-read or <i>put</i> operations, invoke
     * this method to prepare for a sequence of channel-write or relative
     * <i>get</i> operations.  For example:
     *
     * <blockquote><pre>
     * buf.put(magic);    // Prepend header
     * in.read(buf);      // Read data into rest of buffer
     * buf.flip();        // Flip buffer
     * out.write(buf);    // Write header + data to channel</pre></blockquote>
     *
     * <p> This method is often used in conjunction with the {@link
     * java.nio.ByteBuffer#compact compact} method when transferring data from
     * one place to another.  </p>
     *
     * @return  This buffer
     */
    public final NativeMappedMemory flip() {
        limit = position;
        position = 0;
        mark = -1;
        return this;
    }

    /**
     * Rewinds this buffer.  The position is set to zero and the mark is
     * discarded.
     *
     * <p> Invoke this method before a sequence of channel-write or <i>get</i>
     * operations, assuming that the limit has already been set
     * appropriately.  For example:
     *
     * <blockquote><pre>
     * out.write(buf);    // Write remaining data
     * buf.rewind();      // Rewind buffer
     * buf.get(array);    // Copy data into array</pre></blockquote>
     *
     * @return  This buffer
     */
    public final NativeMappedMemory rewind() {
        position = 0;
        mark = -1;
        return this;
    }

    /**
     * Returns the number of elements between the current position and the
     * limit.
     *
     * @return  The number of elements remaining in this buffer
     */
    public final long remaining() {
        return limit - position;
    }

    /**
     * Tells whether there are any elements between the current position and
     * the limit.
     *
     * @return  <tt>true</tt> if, and only if, there is at least one element
     *          remaining in this buffer
     */
    public final boolean hasRemaining() {
        return position < limit;
    }


    public void close() {
        unmap(buffer);
        mapped = false;
    }

    private static void unmap(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null)
            cl.clean();
    }

    public long positionAddress() {
        return address(position);
    }

    public long address(long index) {
        return addressOffset+index;
    }

    public ByteOrder order() {
        return ByteUtils.NATIVE_BYTE_ORDER;
    }

    private void validatePosition(long numberOfBytes) {
        validatePosition(position, numberOfBytes);
    }

    private void validatePosition(long offset, long numberOfBytes) {
        if ( ( (offset+numberOfBytes) > capacity) ) {
            StringBuilder sb = new StringBuilder();
            sb.append("[offset=").append(offset).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Byte                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public byte get()
    {
        validatePosition(BitUtil.SIZE_OF_BYTE);
        final byte value = ByteUtils.UNSAFE.getByte(positionAddress());
        position += BitUtil.SIZE_OF_BYTE;
        return value;
    }



    public byte get(long offset)
    {
        validatePosition(offset, BitUtil.SIZE_OF_BYTE);
        final byte value = ByteUtils.UNSAFE.getByte(address(offset));
        return value;
    }

    public NativeMappedMemory put(byte value) {
        validatePosition(BitUtil.SIZE_OF_BYTE);
        ByteUtils.UNSAFE.putByte(positionAddress(), value);
        position += BitUtil.SIZE_OF_BYTE;
        return this;
    }

    public NativeMappedMemory put(long offset, byte value) {
        validatePosition(offset, BitUtil.SIZE_OF_BYTE);
        ByteUtils.UNSAFE.putByte(address(offset), value);
        return this;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Short                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * Retrieves a two-byte short starting at the current position.
     * @return
     */
    public short getShort() {
        validatePosition(BitUtil.SIZE_OF_SHORT);
        final short value = ByteUtils.UNSAFE.getShort(positionAddress());
        position+=BitUtil.SIZE_OF_SHORT;
        return value;
    }

    /**
     * Retrieves a two-byte short starting at the current position.
     * @return
     */
    public short getShort(long offset) {
        validatePosition(offset, BitUtil.SIZE_OF_SHORT);
        final short value = ByteUtils.UNSAFE.getShort(address(offset));
        return value;
    }

    /**
     *  Stores a two-byte short starting at the current position.
     */
    public NativeMappedMemory putShort(short value) {
        validatePosition(BitUtil.SIZE_OF_SHORT);
        ByteUtils.UNSAFE.putShort(positionAddress(), value);
        position += BitUtil.SIZE_OF_SHORT;
        return this;
    }

    /**
     *  Stores a two-byte short starting at the specified index.
     */
    public NativeMappedMemory putShort(long offset, short value) {
        validatePosition(offset, BitUtil.SIZE_OF_SHORT);
        ByteUtils.UNSAFE.putShort(address(offset), value);
        return this;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Integer                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * Retrieves a four-byte int starting at the current position.
     * @return
     */
    public int getInt() {
        validatePosition(BitUtil.SIZE_OF_INT);
        final int value = ByteUtils.UNSAFE.getInt(positionAddress());
        position+=BitUtil.SIZE_OF_INT;
        return value;
    }

    /**
     * Retrieves a four-byte int starting at the current position.
     * @return
     */
    public int getInt(long offset) {
        validatePosition(offset, BitUtil.SIZE_OF_INT);
        final int value = ByteUtils.UNSAFE.getInt(address(offset));
        return value;
    }

    /**
     *  Stores a four-byte int starting at the current position.
     */
    public NativeMappedMemory putInt(int value) {
        validatePosition(BitUtil.SIZE_OF_INT);
        ByteUtils.UNSAFE.putInt(positionAddress(), value);
        position += BitUtil.SIZE_OF_INT;
        return this;
    }

    /**
     *  Stores a four-byte int starting at the specified index.
     */
    public NativeMappedMemory putInt(long offset, int value) {
        validatePosition(offset, BitUtil.SIZE_OF_INT);
        ByteUtils.UNSAFE.putInt(address(offset), value);
        return this;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Long                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * Retrieves a eight-byte long starting at the current position.
     * @return
     */
    public long getLong() {
        validatePosition(BitUtil.SIZE_OF_LONG);
        final long value = ByteUtils.UNSAFE.getLong(positionAddress());
        position+=BitUtil.SIZE_OF_LONG;
        return value;
    }

    /**
     * Retrieves a eight-byte long starting at the current position.
     * @return
     */
    public long getLong(long offset) {
        validatePosition(offset, BitUtil.SIZE_OF_LONG);
        final long value = ByteUtils.UNSAFE.getLong(address(offset));
        return value;
    }

    /**
     *  Stores a eight-byte long starting at the current position.
     */
    public NativeMappedMemory putLong(long value) {
        validatePosition(BitUtil.SIZE_OF_LONG);
        ByteUtils.UNSAFE.putLong(positionAddress(), value);
        position += BitUtil.SIZE_OF_LONG;
        return this;
    }

    /**
     *  Stores a eight-byte long starting at the specified index.
     */
    public NativeMappedMemory putShort(long offset, long value) {
        validatePosition(offset, BitUtil.SIZE_OF_LONG);
        ByteUtils.UNSAFE.putLong(address(offset), value);
        return this;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Byte Array                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public int getBytes(byte[] destination) {
        return getBytes(destination, 0, destination.length);
    }

    public int getBytes(byte[] destination, int off, int len) {
        validatePosition(len);
        if (len < 0 || off < 0 || off + len > destination.length) {
            throw new IllegalArgumentException();
        }
        long left = remaining();
        if (left <= 0) return -1;
        int len2 = (int) Math.min(len, left);
        ByteUtils.UNSAFE.copyMemory(null, positionAddress(), destination, ByteUtils.BYTE_ARRAY_OFFSET + off, len2);
        position += len2;
        return len2;
    }

    public int getBytes(long offset, byte[] destination) {
        return getBytes(offset, destination, 0, destination.length);
    }

    public int getBytes(long offset, byte[] destination, int off, int len) {
        validatePosition(offset, len);
        if (len < 0 || off < 0 || off + len > destination.length)
            throw new IllegalArgumentException();
        long left = remaining();
        if (left <= 0) return -1;
        int len2 = (int) Math.min(len, left);
        ByteUtils.UNSAFE.copyMemory(null, address(offset), destination, ByteUtils.BYTE_ARRAY_OFFSET + off, len2);
        return len2;
    }

    public int putBytes(byte[] source) {
        return putBytes(source, 0, source.length);
    }

    public int putBytes(byte[] source, int off, int len) {
        validatePosition(len);
        ByteUtils.UNSAFE.copyMemory(source, ByteUtils.BYTE_ARRAY_OFFSET + off, null, positionAddress(), len);
        position += len;
        return len;
    }

    public int putBytes(long offset, byte[] source) {
        return putBytes(offset, source, 0, source.length);
    }

    public int putBytes(long offset, byte[] source, int off, int len) {
        validatePosition(offset, len);
        ByteUtils.UNSAFE.copyMemory(source, ByteUtils.BYTE_ARRAY_OFFSET + off, null, address(offset), len);
        return len;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* ByteBuffer                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public int getBytes(ByteBuffer destination) {
        final int destinationPosition = destination.position();
        final int count = getBytes(destination, destinationPosition, destination.remaining());
        destination.position(destinationPosition+count);
        return count;
    }

    /**
     * Relative bulk <i>get</i> method.
     *
     * <p>This method transfers bytes from this buffer into the given
     * destination {@link java.nio.ByteBuffer}.
     * An invocation of this method of the form
     * <tt>src.get(a)</tt> behaves in exactly the same way as the invocation
     *
     * <pre>
     *     src.get(a, a.position(), length) </pre>
     *
     * @see #getBytes(java.nio.ByteBuffer, int, int)
     *
     * @param destination the destination {@link java.nio.ByteBuffer}
     * @param length
     * @return count of bytes copied.
     */
    public int getBytes(ByteBuffer destination, int length) {
        final int destinationPosition = destination.position();
        final int count = getBytes(destination, destinationPosition, length);
        destination.position(destinationPosition+count);
        return count;
    }

    /**
     * Relative bulk <i>get</i> method.
     *
     * <p> This method transfers bytes from this underlying native memory position into the given
     * destination {@link java.nio.ByteBuffer}.
     * If there are fewer bytes remaining in the buffer than are required to satisfy the request,
     * that is, if
     * <tt>length</tt>&nbsp;<tt>&gt;</tt>&nbsp;<tt>remaining()</tt>, then no
     * bytes are transferred and a {@link java.nio.BufferUnderflowException} is
     * thrown.
     *
     * <p> Otherwise, this method copies <tt>length</tt> bytes from this
     * underlying native memory position into the given destination {@link java.nio.ByteBuffer},
     * starting at the current position of this
     * native memory and at the given offset in the destination {@link java.nio.ByteBuffer}.
     * The position of this native memory is then incremented by <tt>length</tt>.
     *
     * <p> In other words, an invocation of this method of the form
     * <tt>src.getBytes(dst,&nbsp;off,&nbsp;len)</tt> has exactly the same effect as
     * the loop
     *
     * <pre>{@code
     *     for (int i = off; i < off + len; i++)
     *         dst[i] = src.getByte():
     * }</pre>
     *
     * except that it first checks that there are sufficient bytes in
     * this buffer and it is potentially much more efficient.
     *
     * @param destination
     * @param destinationPosition
     * @param length
     * @return
     */
    public int getBytes(ByteBuffer destination, int destinationPosition, int length) {
        validatePosition(length);
        final int destinationPositionAndLength = destinationPosition + length;
        if (length < 0 || destinationPosition < 0 ||
            (destinationPositionAndLength > destination.capacity()) ||
            (length > remaining()) ) {
            StringBuilder sb = new StringBuilder();
            sb.append("[length=").append(length).append("]")
              .append("[destinationPosition=").append(destinationPosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[destinationPositionAndLength=").append(destinationPositionAndLength).append("]")
              .append("[remaining=").append(remaining()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        int count = Integer.min(destination.capacity() - destinationPosition, remainingCapacityAsInt(position));
        count = Math.min(length, count);
        if (count <= 0) return -1;

        final long dstBaseOffset;
        if (destination.hasArray())
        {
            dstBaseOffset = ByteUtils.BYTE_ARRAY_OFFSET + destination.arrayOffset();
        }
        else
        {
            dstBaseOffset = ((sun.nio.ch.DirectBuffer)destination).address();
        }
        final long destAddress = dstBaseOffset + destinationPosition;
        assert( positionAddress()+count <= capacityAddressOffset );
        ByteUtils.UNSAFE.copyMemory(positionAddress(), destAddress, count);
        position += count;
        return count;
    }

    public int getBytes(long offset, ByteBuffer destination) {
        final int destinationPosition = destination.position();
        final int count = getBytes(offset, destination, destinationPosition, destination.remaining());
        destination.position(destinationPosition+count);
        return count;
    }

    public int getBytes(long offset, ByteBuffer destination, int length) {
        final int destinationPosition = destination.position();
        final int count = getBytes(offset, destination, destinationPosition, length);
        destination.position(destinationPosition+count);
        return count;
    }

    public int getBytes(long offset, ByteBuffer destination, int destinationPosition, int length) {
        validatePosition(offset, length);
        final int destinationPositionAndLength = destinationPosition + length;
        final long offsetAndLength = offset + length;
        if (length < 0 || destinationPosition < 0 ||
                (offset > capacity()) ||
                (destinationPositionAndLength > destination.capacity()) ||
                (offsetAndLength > capacity())) {
            StringBuilder sb = new StringBuilder();
            sb.append("[offset=").append(offset).append("]")
              .append("[destinationPosition=").append(destinationPosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[destinationPositionAndLength=").append(destinationPositionAndLength).append("]")
              .append("[destinationCapacity=").append(destination.capacity()).append("]")
              .append("[offsetAndLength=").append(offsetAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        int count = Integer.min(destination.capacity()-destinationPosition, remainingCapacityAsInt(offset));
        count = (int) Math.min(length, count);
        if (count <= 0) return -1;

        final long dstBaseOffset;
        if (destination.hasArray())
        {
            dstBaseOffset = ByteUtils.BYTE_ARRAY_OFFSET + destination.arrayOffset();
        }
        else
        {
            dstBaseOffset = ((sun.nio.ch.DirectBuffer)destination).address();
        }
        final long destAddress = dstBaseOffset + destinationPosition;
        assert(address(offset)+count <= capacityAddressOffset);
        ByteUtils.UNSAFE.copyMemory(address(offset), destAddress, count);
        return count;
    }

    public int putBytes(final ByteBuffer source)
    {
        final int sourcePosition = source.position();
        final int count = putBytes(source, sourcePosition, source.remaining());
        source.position(sourcePosition+count);
        return count;
    }

    public int putBytes(final ByteBuffer source, final int length)
    {
        final int sourcePosition = source.position();
        final int count = putBytes(source, sourcePosition, length);
        source.position(sourcePosition+count);
        return count;
    }

    /**
     * Put an bytes into the underlying buffer for the view.  Bytes will be copied from current
     * {@link java.nio.ByteBuffer#position()} to {@link java.nio.ByteBuffer#limit()}.
     *
     * @param source to copy the bytes from.
     * @param sourcePosition
     * @return count of bytes copied.
     */
    public int putBytes(final ByteBuffer source, int sourcePosition, final int length)
    {
        validatePosition(length);
        final long positionAndLength = position + length;
        if (positionAndLength > capacity()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[position=").append(position).append("]")
              .append("[sourcePosition=").append(sourcePosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[positionAndLength=").append(positionAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        int count = Integer.min(source.capacity()-sourcePosition, remainingCapacityAsInt(position));
        count = Integer.min(count, length);
        if (count <= 0) return -1;

        final int srcOffset = sourcePosition;
        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (source.hasArray())
        {
            srcByteArray = source.array();
            srcBaseOffset = ByteUtils.BYTE_ARRAY_OFFSET + source.arrayOffset();
        }
        else
        {
            srcByteArray = null;
            srcBaseOffset = ((sun.nio.ch.DirectBuffer)source).address();
        }

        final long srcAddress = srcBaseOffset + srcOffset;
        assert (positionAddress()+count <= capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(srcByteArray, srcAddress, byteArray, positionAddress(),  count);
        position += count;
        return (int)count;
    }

    public int putBytes(final long offset, final ByteBuffer source)
    {
        final int sourcePosition = source.position();
        final int count = putBytes(offset, source, sourcePosition, source.remaining());
        source.position(sourcePosition+count);
        return count;
    }

    public int putBytes(final long offset, final ByteBuffer source, final int length)
    {
        final int sourcePosition = source.position();
        final int count = putBytes(offset, source, sourcePosition, length);
        source.position(sourcePosition+count);
        return count;
    }

    /**
     * Put an bytes into the underlying buffer for the view.  Bytes will be copied from current
     * {@link java.nio.ByteBuffer#position()} to {@link java.nio.ByteBuffer#limit()}.
     *
     * @param offset     in the underlying native memory to start from.
     * @param source to copy the bytes from.
     * @param sourcePosition
     * @return count of bytes copied.
     */
    public int putBytes(final long offset, final ByteBuffer source, int sourcePosition, final int length)
    {
        validatePosition(offset, length);
        final long offsetAndLength = offset + length;
        if (offset < 0 || offsetAndLength > capacity()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[offset=").append(offset).append("]")
              .append("[sourcePosition=").append(sourcePosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[sourceRemaining=").append(source.remaining()).append("]")
              .append("[offsetAndLength=").append(offsetAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        int count = Integer.min(source.capacity()-sourcePosition, remainingCapacityAsInt(offset));
        count = Math.min(count, length);
        if (count <= 0) return -1;

        final int srcOffset = sourcePosition;
        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (source.hasArray())
        {
            srcByteArray = source.array();
            srcBaseOffset = ByteUtils.BYTE_ARRAY_OFFSET + source.arrayOffset();
        }
        else
        {
            srcByteArray = null;
            srcBaseOffset = ((sun.nio.ch.DirectBuffer)source).address();
        }

        final long srcAddress = srcBaseOffset + srcOffset;
        assert(address(offset)+count<=capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(srcByteArray, srcAddress, byteArray, address(offset),  count);
        return count;
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* NativeMappedMemory                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public long remainingCapacity(long offset) {
        return capacity - offset;
    }

    public int remainingCapacityAsInt(long offset) {
        return (int)Long.min(capacity - offset, Integer.MAX_VALUE);
    }

    public long getBytes(final NativeMappedMemory destination) {
        final long destinationPosition = destination.position();
        final long count = getBytes(destination, destinationPosition, destination.remaining());
        destination.position(destinationPosition+count);
        return count;
    }

    public long getBytes(final NativeMappedMemory destination, long length) {
        final long destinationPosition = destination.position();
        final long count = getBytes(destination, destinationPosition, length);
        destination.position(destinationPosition+count);
        return count;
    }

    public long getBytes(final NativeMappedMemory  destination, long destinationPosition, long length) {
        validatePosition(length);
        final long destinationPositionAndLength = destinationPosition + length;
        if (length < 0 || destinationPosition < 0 ||
            (destinationPositionAndLength > destination.capacity()) ||
            (length > remaining()) ) {
            StringBuilder sb = new StringBuilder();
            sb.append("[length=").append(length).append("]")
              .append("[destinationPosition=").append(destinationPosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[destinationPositionAndLength=").append(destinationPositionAndLength).append("]")
              .append("[remaining=").append(remaining()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        long count = Long.min(destination.remainingCapacity(destinationPosition), remainingCapacity(position));
        count = Long.min(length, count);
        if (count <= 0) return -1;

        final long dstBaseOffset = destination.addressOffset();
        final long destAddress = dstBaseOffset + destinationPosition;
        assert( positionAddress()+count <= capacityAddressOffset );
        assert (destAddress+count <= destination.capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(positionAddress(), destAddress, count);
        position += count;
        return count;
    }

    public long getBytes(long offset, NativeMappedMemory destination) {
        final long destinationPosition = destination.position();
        final long count = getBytes(offset, destination, destinationPosition, destination.remaining());
        destination.position(destinationPosition+count);
        return count;
    }

    public long getBytes(long offset, NativeMappedMemory destination, long length) {
        final long destinationPosition = destination.position();
        final long count = getBytes(offset, destination, destinationPosition, length);
        destination.position(destinationPosition+count);
        return count;
    }

    public long getBytes(long offset, final NativeMappedMemory  destination, long destinationPosition, long length) {
        validatePosition(offset, length);
        final long destinationPositionAndLength = destinationPosition + length;
        final long offsetAndLength = offset + length;
        if (length < 0 || destinationPosition < 0 ||
            (offset > capacity()) ||
            (destinationPositionAndLength > destination.capacity()) ||
            (offsetAndLength > capacity())) {
            StringBuilder sb = new StringBuilder();
            sb.append("[offset=").append(offset).append("]")
              .append("[destinationPosition=").append(destinationPosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[destinationPositionAndLength=").append(destinationPositionAndLength).append("]")
              .append("[destinationCapacity=").append(destination.capacity()).append("]")
              .append("[offsetAndLength=").append(offsetAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        long count = Long.min(destination.remainingCapacity(destinationPosition),remainingCapacity(offset));
        count = Long.min(count, length);
        if (count <= 0) return -1;

        final long dstBaseOffset = destination.addressOffset();
        final long destAddress = dstBaseOffset + destinationPosition;
        assert(address(offset)+count <= capacityAddressOffset);
        assert (destAddress+count <= destination.capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(address(offset), destAddress, count);
        position += count;
        return count;
    }

    public long putBytes(final NativeMappedMemory source)
    {
        final long sourcePosition = source.position();
        final long count = putBytes(source, sourcePosition, source.limit());
        source.position(sourcePosition+count);
        return count;
    }

    public long putBytes(final NativeMappedMemory source, final long length)
    {
        final long sourcePosition = source.position();
        final long count = putBytes(source, sourcePosition, length);
        source.position(sourcePosition+count);
        return count;
    }

    public long putBytes(final NativeMappedMemory source, long sourcePosition, final long length)
    {
        validatePosition(length);
        final long positionAndLength = position + length;
        if (positionAndLength > capacity()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[position=").append(position).append("]")
              .append("[sourcePosition=").append(sourcePosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[positionAndLength=").append(positionAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        long count = Math.min(source.remainingCapacity(sourcePosition), remainingCapacity(position));
        count = Math.min(count, length);
        if (count <= 0) return -1;

        final long srcOffset = sourcePosition;
        final long srcBaseOffset = source.addressOffset();
        final long srcAddress = srcBaseOffset + srcOffset;
        assert (positionAddress()+count <= capacityAddressOffset());
        assert (srcAddress+count <= source.capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(srcAddress, positionAddress(),  count);
        position += count;
        return count;
    }

    public long putBytes(final long offset, final NativeMappedMemory source)
    {
        final long sourcePosition = source.position();
        final long count = putBytes(offset, source, sourcePosition, source.remaining());
        source.position(sourcePosition+count);
        return count;
    }

    public long putBytes(final long offset, final NativeMappedMemory source, final long length)
    {
        final long sourcePosition = source.position();
        final long count = putBytes(offset, source, sourcePosition, length);
        source.position(sourcePosition+count);
        return count;
    }

    public long putBytes(final long offset, final NativeMappedMemory source, long sourcePosition, final long length)
    {
        validatePosition(offset, length);
        final long offsetAndLength = offset + length;
        if (offset < 0 || offsetAndLength > capacity()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[offset=").append(offset).append("]")
              .append("[sourcePosition=").append(sourcePosition).append("]")
              .append("[length=").append(length).append("]")
              .append("[sourceRemaining=").append(source.remaining()).append("]")
              .append("[offsetAndLength=").append(offsetAndLength).append("]")
              .append("[capacity=").append(capacity()).append("]")
            ;
            throw new IllegalArgumentException(sb.toString());
        }
        long count = Math.min(source.remainingCapacity(sourcePosition), remainingCapacity(offset));
        count = Math.min(count, length);
        if (count <= 0) return -1;

        final long srcOffset = sourcePosition;
        final long srcBaseOffset = source.addressOffset();
        final long srcAddress = srcBaseOffset + srcOffset;
        assert (address(offset)+count <= capacityAddressOffset());
        assert (srcAddress+count <= source.capacityAddressOffset());
        ByteUtils.UNSAFE.copyMemory(srcAddress, address(offset),  count);
        return count;
    }


}
