// Copyright Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.rsm.buffer;

import com.rsm.util.ByteUnit;
import com.rsm.util.ByteUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 *  A wrapper for memory-mapped files that generally preserves the semantics of
 *  <code>ByteBuffer</code>, while supporing files larger than 2 GB. Unlike
 *  normal byte buffers, all access via absolute index, and indexes are
 *  <code>long</code> values.
 *  <p>
 *  This is achieved using a set of overlapping buffers, based on the "segment
 *  size" passed during construction. Segment size is the largest contiguous
 *  sub-buffer that may be accessed (via {@link #getBytes} and {@link #putBytes}),
 *  and may be no larger than 1 GB.
 *  <p>
 *  <strong>Warning:</strong>
 *  This class is not thread-safe. Caller must explicitly synchronize access,
 *  or call {@link #clone} to create a distinct buffer for each thread.
 *
 *  @see net.sf.kdgcommons.buffer.MappedFileBuffer
 */
public class NativeMappedFileBuffer
implements Bytes, Cloneable
{

    private static final Logger log = LogManager.getLogger(NativeMappedFileBuffer.class);

//    public static final long NO_PAGE = ByteUtils.UNSAFE.allocateMemory(ByteUtils.PAGE_SIZE);

    public final static long DEFAULT_SEGMENT_SIZE = ByteUnit.MEGABYTE.getBytes();
//    public final static int MAX_SEGMENT_SIZE = 0x40000000; // 1 GB, assures alignment
    public final static int MAX_SEGMENT_SIZE = maxSegmentSize();

    /**
     * Ensure it is page aligned
     * @return
     */
    private static int maxSegmentSize() {
        final long remainder = (long) Integer.MAX_VALUE % ByteUtils.PAGE_SIZE;
        long maxSegmentSize = Integer.MAX_VALUE - remainder;
        return (int)maxSegmentSize;
    }


    private File _file;
    private boolean _isWritable;
    private long _initialFileSize;
    private long _initialSegmentSize;              // long because it's used in long expressions
    private long _segmentSize;              // long because it's used in long expressions
//    private long _growBySize;
    private NativeMappedMemory[] _buffers;
    private long position = 0;
    private NativeMappedMemory currentByteBuffer;
    private int currentIndex;
    private final String mode;
    private final MapMode mapMode;
    private RandomAccessFile _mappedFile;
    private FileChannel _channel;

    public void print(StringBuilder sb) {
        int buffersLength = 0;
        if(_buffers != null) {
            buffersLength =  _buffers.length;
        }
        sb.append("[position=").append(position).append(" ")
//                .append("limit=").append(limit).append(" ")
          .append("capacity=").append(capacity()).append(" ")

          .append("segmentSize=").append(_segmentSize).append(" ")
          .append("currentSegment=").append(currentIndex).append(" ")
          .append("segments=").append(buffersLength)
          .append("]")
        ;
    }

    public long getBufferPosition(long position) {
        return (position % _segmentSize);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        print(sb);
        return sb.toString();
    }

    /**
     *  Opens and memory-maps the specified file for read-only access, using
     *  the maximum segment size.
     *
     *  @param  file        The file to open; must be accessible to user.
     *
     *  @throws IllegalArgumentException if <code>segmentSize</code> is > 1GB.
     */
    public NativeMappedFileBuffer(File file)
    throws IOException
    {
        this(file, DEFAULT_SEGMENT_SIZE, DEFAULT_SEGMENT_SIZE, true, false);
    }


    /**
     *  Opens and memory-maps the specified file for read-only or read-write
     *  access, using the maximum segment size.
     *
     *  @param  file        The file to open; must be accessible to user.
     *  @param  readWrite   Pass <code>true</code> to open the file with
     *                      read-write access, <code>false</code> to open
     *                      with read-only access.
     *
     *  @throws IllegalArgumentException if <code>segmentSize</code> is > 1GB.
     */
    public NativeMappedFileBuffer(File file, boolean readWrite)
    throws IOException
    {
        this(file, DEFAULT_SEGMENT_SIZE, DEFAULT_SEGMENT_SIZE, readWrite, false);
    }


    /**
     *  Opens and memory-maps the specified file, for read-only or read-write
     *  access, with a specified segment size.
     *
     *  @param  file        The file to open; must be accessible to user.
     *  @param  segmentSize The largest contiguous sub-buffer that can be
     *                      created using {@link #slice}. The maximum size
     *                      is 2^30 - 1.
     *  @param  readWrite   Pass <code>true</code> to open the file with
     *                      read-write access, <code>false</code> to open
     *                      with read-only access.
     *
     *  @throws IllegalArgumentException if <code>segmentSize</code> is > 1GB.
     */
    public NativeMappedFileBuffer(File file, long segmentSize, long initialFileSize, boolean readWrite, boolean deleteFileIfExists)
    throws IOException
    {
        if (segmentSize > MAX_SEGMENT_SIZE)
            throw new IllegalArgumentException(
                    "segment size too large (max is " + MAX_SEGMENT_SIZE + "): " + segmentSize);

        if(initialFileSize <= 0) {
            throw new IllegalArgumentException("Invalid initial File Size " + initialFileSize);
        }

        if(deleteFileIfExists && file.exists()) {
            boolean deleted = file.delete();
            if(!deleted) {
                log.warn("Unable to delete file " + file.getAbsoluteFile());
            }
            else {
                boolean created = file.createNewFile();
                if(!created) {
                    log.warn("Unable to recreate file " + file.getAbsolutePath());
                }
            }
        }


        _file = file;
        _isWritable = readWrite;
        _initialSegmentSize = segmentSize;
        _initialFileSize = initialFileSize;

        //force to be in page_aligned units
        final long segmentSizeRemainder = _initialSegmentSize % ByteUtils.PAGE_SIZE;
        final long tempSegmentSize = _initialSegmentSize + segmentSizeRemainder;
        _segmentSize = Long.min(tempSegmentSize, MAX_SEGMENT_SIZE);
        if(_initialSegmentSize != _segmentSize) {
            log.info("Adjusting segment size to be in page aligned units. From " + _initialSegmentSize + " to " + _segmentSize + " bytes");
        }
        try
        {
            this.mode = readWrite ? "rw" : "r";
            this.mapMode = readWrite ? MapMode.READ_WRITE : MapMode.READ_ONLY;

            _mappedFile = new RandomAccessFile(file, mode);
            _channel = _mappedFile.getChannel();

            //make each segment equal sizes
            long fileSize = Math.max(_segmentSize, capacity());
            long remainder = fileSize % _segmentSize;
            fileSize = _initialFileSize + remainder;
            if(fileSize <= 0) {
                throw new IllegalArgumentException("Invalid File Size " + fileSize);
            }

            long bufArraySizeLong = (int)(fileSize / segmentSize)
                             + ((fileSize % segmentSize != 0) ? 1 : 0);
            if(bufArraySizeLong > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(bufArraySizeLong + " segments is greater than allowed count of " + Integer.MAX_VALUE);
            }
            int bufArraySize = (int)bufArraySizeLong;
            _buffers = new NativeMappedMemory[bufArraySize];
            int bufIdx = 0;
            for (long offset = 0 ; offset < fileSize ; offset += segmentSize)
            {
                long remainingFileSize = fileSize - offset;
                long thisSegmentSize = Math.min(segmentSize, remainingFileSize);
                final NativeMappedMemory nativeMappedMemory = NativeMappedMemory.create(thisSegmentSize);
                _buffers[bufIdx++] = nativeMappedMemory;
            }
            position = 0;
            processPosition();
            //preload first segment
            nativeMappedMemory(position);
        }
        finally
        {
//            IOUtil.closeQuietly(mappedFile);
        }
    }

    private void grow(int index) {
        if(index >= _buffers.length) {
            long capacity = capacity();
            long fileSize = capacity() + _segmentSize;
            long remainder = fileSize % _segmentSize;
            fileSize += remainder;

//            MapMode mapMode = _isWritable ? MapMode.READ_WRITE : MapMode.READ_ONLY;

            long bufArraySizeLong = (fileSize / _segmentSize)
                    + ((fileSize % _segmentSize != 0) ? 1 : 0);
            if(bufArraySizeLong > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(bufArraySizeLong + " segments is greater than allowed count of " + Integer.MAX_VALUE);
            }
            int bufArraySize = (int)bufArraySizeLong;
            log.info("growing from [segmentSize="+_segmentSize+"] [capacity="+capacity+"][numOfSegment=" + _buffers.length + "] to " +
                    "[capacity="+fileSize+"][numOfSegments=" + bufArraySize+"]");
            NativeMappedMemory[] temp = new NativeMappedMemory[bufArraySize];
            int bufIdx = 0;
            for (long offset = 0 ; offset < fileSize ; offset += _segmentSize)
            {
                if(bufIdx < _buffers.length) {
                    try {
                        final NativeMappedMemory nativeMappedMemory = _buffers[bufIdx];
                        if(nativeMappedMemory.isMapped()) {

                        }
                        else {
                            nativeMappedMemory.release();
                        }
                        nativeMappedMemory.clear();
                        temp[bufIdx] = nativeMappedMemory;
                    }
                    catch(Exception e) {
                        log.warn("Unable to memory-map file at index " + bufIdx, e);
                    }
                }
                else {
                    map(temp, bufIdx, _segmentSize);
                }
                bufIdx++;
            }
            _buffers = temp;
        }
    }

     private void map(NativeMappedMemory[] directMappedMemories, int bufIdx, long thisSegmentSize) {
         try {
             //directMappedMemories
             final NativeMappedMemory nativeMappedMemory = NativeMappedMemory.create(thisSegmentSize);
             directMappedMemories[bufIdx] = nativeMappedMemory;
         }
         catch(IOException e) {
             log.warn("Unable to memory-map file at index " + bufIdx, e);
         }
     }

//    private void map(MapMode mapMode, MappedByteBuffer[] mappedByteBuffer, int bufIdx, long offset, long thisSegmentSize) {
//        try {
//            mappedByteBuffer[bufIdx] = MapUtils.getMap(_channel, mapMode, offset, thisSegmentSize);
//            mappedByteBuffer[bufIdx].order(ByteUtils.NATIVE_BYTE_ORDER);
//        }
//        catch(IOException e) {
//            log.warn("Unable to memory-map file at index " + bufIdx, e);
//        }
//    }

    public static class MapUtils {
        public static MappedByteBuffer getMap(FileChannel fileChannel, MapMode mapMode, long start, long size) throws  IOException {
            for(int i=1; ; i++) {
                try {
                    if(!fileChannel.isOpen()) {
                        continue;
                    }
                    MappedByteBuffer map = fileChannel.map(mapMode, start, size);
                    return map;
                }
                catch(IOException e) {
                    if(e.getMessage() == null || !e.getMessage().endsWith("user-mapped section open")) {
                        throw e;
                    }
                    if(i<10) {
                        //noinspection CallToThreadYield
                        Thread.yield();
                    }
                    else {
                        try {
                            //noinspection BusyWait
                            Thread.sleep(1);
                        }
                        catch(InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                    }
                }
            }
        }
    }


//----------------------------------------------------------------------------
//  Public methods
//----------------------------------------------------------------------------


    public long segmentSize() {
        return _segmentSize;
    }

    public long initialFileSize() {
        return _initialFileSize;
    }

    public long initialSegmentSize() {
        return _initialSegmentSize;
    }

    public RandomAccessFile getRandomAccessFile() {
        return _mappedFile;
    }

    public FileChannel getFileChannel() {
        return _channel;
    }

    public long position() {
        return position;
    }

    public NativeMappedFileBuffer position(long position) {
        this.position = position;
        processPosition();
        return this;
    }

    /**
     *  Returns the buffer's capacity -- the size of the mapped file.
     */
    public long capacity()
    {
        try {
            return _mappedFile.length();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     *  Returns the buffer's limit -- the maximum index in the buffer + 1.
     *  <p>
     *  This returns the same value as {@link #capacity}; it exists as part of
     *  the {@link Bytes} interface.
     */
    public long limit()
    {
        return capacity();
    }


    /**
     *  Returns the file that is mapped by this buffer.
     */
    public File file()
    {
        return _file;
    }


    /**
     *  Indicates whether this buffer is read-write or read-only.
     */
    public boolean isWritable()
    {
        return _isWritable;
    }


    /**
     *  Returns the byte-order of this buffer (actually, the order of the first
     *  child buffer; they should all be the same).
     */
    public ByteOrder getByteOrder()
    {
        return ByteUtils.NATIVE_BYTE_ORDER;
//        return _buffers[0].order();
    }


//    /**
//     *  Sets the order of this buffer (propagated to all child buffers).
//     */
//    public void setByteOrder(ByteOrder order)
//    {
//        for (ByteBuffer child : _buffers)
//            child.order(order);
//    }

    @Override
    public int getCurrentIndex() {
        return currentIndex;
    }

    private void processPosition() {
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = nativeMappedMemory(position);
    }

//    public long positionAddress() {
//        return address(position);
//    }
//
//    public long address(long index) {
//        return currentByteBuffer.addressOffset()+index;
//    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Byte                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     *  Retrieves a single byte from the current position.
     */
    @Override
    public byte get()
    {
        long oldPosition = position;
        byte value = currentByteBuffer.get();
        position += 1;
        processPosition();
        unmap(oldPosition, position);
        return value;
    }

    @Override
    public byte get(long index) {
        long oldPosition = index;
        final byte value = nativeMappedMemory(index).get();
        index += 1;
        processPosition();
        unmap(oldPosition, index);
        return value;
    }

    @Override
    public void put(byte value) {
        long oldPosition = position;
        currentByteBuffer.put(value);
        position += 1;
        processPosition();
        unmap(oldPosition, position);
    }

    /**
     *  Stores a single byte at the specified index.
     */
    @Override
    public void put(long index, byte value)
    {
        long oldPosition = index;
        nativeMappedMemory(index).put(value);
        processPosition();
        unmap(oldPosition, index);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Short                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * Retrieves a two-byte short starting at the current position.
     * @return
     */
    public short getShort() {
        return getShort(getByteOrder());
    }

    /**
     * Retrieves a two-byte short starting at the current position.
     * @return
     */
    public short getShort(ByteOrder byteOrder) {
        long oldPosition = position;
        short value = 0;
        final long remaining = currentByteBuffer.remaining();
        if (remaining < 2) {
            byte short0 = 0;
            byte short1 = 0;
            switch ((int)remaining) {
                case 1:
                    short0 = currentByteBuffer.get();
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    short1 = currentByteBuffer.get();
                    position += 1;
                    break;
            }
            value = ByteUtils.makeShort(short1, short0);
        }
        else {
            value = currentByteBuffer.getShort();
            position += 2;
        }
        processPosition();
        unmap(oldPosition, position);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Short.reverseBytes(value);
        }
        return value;
    }

    /**
     * Retrieves a two-byte short starting at the specified position.
     * @return
     */
    public short getShort(long index, ByteOrder byteOrder) {
        long oldPosition = index;
        short value;
        NativeMappedMemory buffer = nativeMappedMemory(index);
        final long remaining = buffer.remaining();
        if (remaining < 2) {
            byte short0 = 0;
            byte short1 = 0;
            switch ((int)remaining) {
                case 1:
                    short0 = buffer.get();
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    short1 = buffer.get();
                    index += 1;
                    break;
            }
            value = ByteUtils.makeShort(short1, short0);
        }
        else {
            value = buffer.getShort();
            index += 2;
        }
        processPosition();
        unmap(oldPosition, index);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Short.reverseBytes(value);
        }
        return value;
    }

    /**
     *  Retrieves a four-byte integer starting at the specified index.
     */
    public short getShort(long index)
    {
        return getShort(index, getByteOrder());
    }

    /**
     *  Stores a two-byte short starting at the specified index.
     */
    public void putShort(long index, short value, ByteOrder byteOrder)
    {
        long oldPosition = index;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Short.reverseBytes(value);
        }
        NativeMappedMemory buffer = nativeMappedMemory(index);
        long remaining = buffer.remaining();
        if(remaining < 2) {
            switch ((int)remaining) {
                case 1:
                    buffer.put(ByteUtils.short0(value));
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.short1(value));
                    index += 1;
                    break;
            }
        }
        else {
            buffer.putShort(value);
            index += 2;
        }
        processPosition();
        unmap(oldPosition, index);
    }

    /**
     *  Stores a two-byte short starting at the specified index.
     */
    public void putShort(long index, short value)
    {
        putShort(index, value, getByteOrder());
    }

    /**
     *  Stores a two-byte short starting at the current position.
     */
    public void putShort(short value, ByteOrder byteOrder)
    {
        long oldPosition = position;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Short.reverseBytes(value);
        }
        long remaining = currentByteBuffer.remaining();
        if(remaining < 2) {
            switch ((int)remaining) {
                case 1:
                    currentByteBuffer.put(ByteUtils.short0(value));
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.short1(value));
                    position += 1;
                    break;
            }
        }
        else {
            currentByteBuffer.putShort(value);
            position += 2;
        }
        processPosition();
        unmap(oldPosition, position);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Integer                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     * Retrieves a four-byte integer starting at the current position.
     * @return
     */
    @Override
    public int getInt() {
        return getInt(getByteOrder());
    }

    /**
     * Retrieves a four-byte integer starting at the current position.
     * @return
     */
    public int getInt(ByteOrder byteOrder) {
        long oldPosition = position;
        int value;
        long remaining = currentByteBuffer.remaining();
        if(remaining < 4) {
            byte int0 = 0;
            byte int1 = 0;
            byte int2 = 0;
            byte int3 = 0;
            switch((int)remaining) {
                case 1:
                    int0 = currentByteBuffer.get();
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    int1 = currentByteBuffer.get();
                    int2 = currentByteBuffer.get();
                    int3 = currentByteBuffer.get();
                    position += 3;
                    break;
                case 2:
                    int0 = currentByteBuffer.get();
                    int1 = currentByteBuffer.get();
                    position += 2;
                    currentByteBuffer = nativeMappedMemory(position);
                    int2 = currentByteBuffer.get();
                    int3 = currentByteBuffer.get();
                    position += 2;
                    break;
                case 3:
                    int0 = currentByteBuffer.get();
                    int1 = currentByteBuffer.get();
                    int2 = currentByteBuffer.get();
                    position += 3;
                    currentByteBuffer = nativeMappedMemory(position);
                    int3 = currentByteBuffer.get();
                    position += 1;
                    break;
            }
            value = ByteUtils.makeInt(int3, int2, int1, int0);
        }
        else {
            value = currentByteBuffer.getInt();
            position += 4;
        }
        processPosition();
        unmap(oldPosition, position);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Integer.reverseBytes(value);
        }
        return value;
    }

    public int getInt(long index) {
        return getInt(index, getByteOrder());
    }

    /**
     *  Retrieves a four-byte integer starting at the specified index.
     */
    public int getInt(long index, ByteOrder byteOrder)
    {
        long oldPosition = index;
        int value;
        NativeMappedMemory buffer = nativeMappedMemory(index);
        long remaining = buffer.remaining();
        if(remaining < 4) {
            byte int0 = 0;
            byte int1 = 0;
            byte int2 = 0;
            byte int3 = 0;
            switch((int)remaining) {
                case 1:
                    int0 = buffer.get();
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    int1 = buffer.get();
                    int2 = buffer.get();
                    int3 = buffer.get();
                    index += 3;
                    break;
                case 2:
                    int0 = buffer.get();
                    int1 = buffer.get();
                    index += 2;
                    buffer = nativeMappedMemory(index);
                    int2 = buffer.get();
                    int3 = buffer.get();
                    index += 2;
                    break;
                case 3:
                    int0 = buffer.get();
                    int1 = buffer.get();
                    int2 = buffer.get();
                    index += 3;
                    buffer = nativeMappedMemory(index);
                    int3 = buffer.get();
                    index += 1;
                    break;
            }
            value = ByteUtils.makeInt(int3, int2, int1, int0);
        }
        else {
            value = buffer.getInt();
            index += 4;
        }
        processPosition();
        unmap(oldPosition, index);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Integer.reverseBytes(value);
        }
        return value;
    }

    @Override
    public void putInt(int value) {
        putInt(value, getByteOrder());
    }

    public void putInt(int value, ByteOrder byteOrder) {
        long oldPosition = position;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Integer.reverseBytes(value);
        }
        long remaining = currentByteBuffer.remaining();
        if(remaining < 4) {
            switch((int)remaining) {
                case 1:
                    currentByteBuffer.put(ByteUtils.int0(value));
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.int1(value));
                    currentByteBuffer.put(ByteUtils.int2(value));
                    currentByteBuffer.put(ByteUtils.int3(value));
                    position += 3;
                    break;
                case 2:
                    currentByteBuffer.put(ByteUtils.int0(value));
                    currentByteBuffer.put(ByteUtils.int1(value));
                    position += 2;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.int2(value));
                    currentByteBuffer.put(ByteUtils.int3(value));
                    position += 2;
                    break;
                case 3:
                    currentByteBuffer.put(ByteUtils.int0(value));
                    currentByteBuffer.put(ByteUtils.int1(value));
                    currentByteBuffer.put(ByteUtils.int2(value));
                    position += 3;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.int3(value));
                    position += 1;
                    break;
            }
        }
        else {
            currentByteBuffer.putInt(value);
            position += 4;
        }
        processPosition();
        unmap(oldPosition, position);
    }

    /**
     *  Stores a four-byte integer starting at the specified index.
     */
    public void putInt(long index, int value) {
        putInt(index, value, getByteOrder());
    }

    /**
     *  Stores a four-byte integer starting at the specified index.
     */
    public void putInt(long index, int value, ByteOrder byteOrder)
    {
        long oldPosition = index;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Integer.reverseBytes(value);
        }
        NativeMappedMemory buffer = nativeMappedMemory(index);
        long remaining = buffer.remaining();
        if(remaining < 4) {
            switch((int)remaining) {
                case 1:
                    buffer.put(ByteUtils.int0(value));
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.int1(value));
                    buffer.put(ByteUtils.int2(value));
                    buffer.put(ByteUtils.int3(value));
                    index += 3;
                    break;
                case 2:
                    buffer.put(ByteUtils.int0(value));
                    buffer.put(ByteUtils.int1(value));
                    index += 2;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.int2(value));
                    buffer.put(ByteUtils.int3(value));
                    index += 2;
                    break;
                case 3:
                    buffer.put(ByteUtils.int0(value));
                    buffer.put(ByteUtils.int1(value));
                    buffer.put(ByteUtils.int2(value));
                    index += 3;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.int3(value));
                    index += 1;
                    break;
            }
        }
        else {
            buffer.putInt(value);
            index += 4;
        }
        processPosition();
        unmap(oldPosition, index);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Long                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public long  getLong() {
        return getLong(getByteOrder());
    }

    public long  getLong(ByteOrder byteOrder) {
        long oldPosition = position;
        long value;
        long remaining = currentByteBuffer.remaining();
        if(remaining < 8) {
            byte long0 = 0;
            byte long1 = 0;
            byte long2 = 0;
            byte long3 = 0;
            byte long4 = 0;
            byte long5 = 0;
            byte long6 = 0;
            byte long7 = 0;
            switch ((int)remaining) {
                case 1:
                    long0 = currentByteBuffer.get();
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 7;
                    break;
                case 2:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    position += 2;
                    currentByteBuffer = nativeMappedMemory(position);
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 6;
                    break;
                case 3:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    position += 3;
                    currentByteBuffer = nativeMappedMemory(position);
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 5;
                    break;
                case 4:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    position += 4;
                    currentByteBuffer = nativeMappedMemory(position);
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 4;
                    break;
                case 5:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    position += 5;
                    currentByteBuffer = nativeMappedMemory(position);
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 3;
                    break;
                case 6:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    position += 6;
                    currentByteBuffer = nativeMappedMemory(position);
                    long6 = currentByteBuffer.get();
                    long7 = currentByteBuffer.get();
                    position += 2;
                    break;
                case 7:
                    long0 = currentByteBuffer.get();
                    long1 = currentByteBuffer.get();
                    long2 = currentByteBuffer.get();
                    long3 = currentByteBuffer.get();
                    long4 = currentByteBuffer.get();
                    long5 = currentByteBuffer.get();
                    long6 = currentByteBuffer.get();
                    position += 7;
                    currentByteBuffer = nativeMappedMemory(position);
                    long7 = currentByteBuffer.get();
                    position += 1;
                    break;
            }
            value = ByteUtils.makeLong(long7, long6, long5, long4, long3, long2, long1, long0);
        }
        else {
            value = currentByteBuffer.getLong();
            position += 8;
        }
        processPosition();
        unmap(oldPosition, position);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Long.reverseBytes(value);
        }
        return value;
    }

    /**
     *  Retrieves an eight-byte integer starting at the specified index.
     */
    public long getLong(long index)
    {
        return getLong(index, getByteOrder());
    }

    public long  getLong(long index, ByteOrder byteOrder) {
        long oldPosition = index;
        long value;
        NativeMappedMemory buffer = nativeMappedMemory(index);
        long remaining = buffer.remaining();
        if(remaining < 8) {
            byte long0 = 0;
            byte long1 = 0;
            byte long2 = 0;
            byte long3 = 0;
            byte long4 = 0;
            byte long5 = 0;
            byte long6 = 0;
            byte long7 = 0;
            switch ((int)remaining) {
                case 1:
                    long0 = buffer.get();
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    long1 = buffer.get();
                    long2 = buffer.get();
                    long3 = buffer.get();
                    long4 = buffer.get();
                    long5 = buffer.get();
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 7;
                    break;
                case 2:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    index += 2;
                    buffer = nativeMappedMemory(index);
                    long2 = buffer.get();
                    long3 = buffer.get();
                    long4 = buffer.get();
                    long5 = buffer.get();
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 6;
                    break;
                case 3:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    long2 = buffer.get();
                    index += 3;
                    buffer = nativeMappedMemory(index);
                    long3 = buffer.get();
                    long4 = buffer.get();
                    long5 = buffer.get();
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 5;
                    break;
                case 4:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    long2 = buffer.get();
                    long3 = buffer.get();
                    index += 4;
                    buffer = nativeMappedMemory(index);
                    long4 = buffer.get();
                    long5 = buffer.get();
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 4;
                    break;
                case 5:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    long2 = buffer.get();
                    long3 = buffer.get();
                    long4 = buffer.get();
                    index += 5;
                    buffer = nativeMappedMemory(index);
                    long5 = buffer.get();
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 3;
                    break;
                case 6:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    long2 = buffer.get();
                    long3 = buffer.get();
                    long4 = buffer.get();
                    long5 = buffer.get();
                    index += 6;
                    buffer = nativeMappedMemory(index);
                    long6 = buffer.get();
                    long7 = buffer.get();
                    index += 2;
                    break;
                case 7:
                    long0 = buffer.get();
                    long1 = buffer.get();
                    long2 = buffer.get();
                    long3 = buffer.get();
                    long4 = buffer.get();
                    long5 = buffer.get();
                    long6 = buffer.get();
                    index += 7;
                    buffer = nativeMappedMemory(index);
                    long7 = buffer.get();
                    index += 1;
                    break;
            }
            value = ByteUtils.makeLong(long7, long6, long5, long4, long3, long2, long1, long0);
        }
        else {
            value = buffer.getLong();
            index += 8;
        }
        processPosition();
        unmap(oldPosition, index);
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Long.reverseBytes(value);
        }
        return value;
    }

    public void putLong(long value)
    {
        putLong(value, getByteOrder());
    }
    /**
     *  Stores an eight-byte integer starting at the current position.
     */
    public void putLong(long value, ByteOrder byteOrder)
    {
        long oldPosition = position;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Long.reverseBytes(value);
        }
        long remaining = currentByteBuffer.remaining();
        if(remaining < 8) {
            switch((int)remaining) {
                case 1:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    position += 1;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 7;
                    break;
                case 2:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    position += 2;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 6;
                    break;
                case 3:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    position += 3;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 5;
                    break;
                case 4:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    position += 4;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 4;
                    break;
                case 5:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    position += 5;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 3;
                    break;
                case 6:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    position += 6;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long6(value));
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 2;
                    break;
                case 7:
                    currentByteBuffer.put(ByteUtils.long0(value));
                    currentByteBuffer.put(ByteUtils.long1(value));
                    currentByteBuffer.put(ByteUtils.long2(value));
                    currentByteBuffer.put(ByteUtils.long3(value));
                    currentByteBuffer.put(ByteUtils.long4(value));
                    currentByteBuffer.put(ByteUtils.long5(value));
                    currentByteBuffer.put(ByteUtils.long6(value));
                    position += 7;
                    currentByteBuffer = nativeMappedMemory(position);
                    currentByteBuffer.put(ByteUtils.long7(value));
                    position += 1;
                    break;
            }
        }
        else {
            currentByteBuffer.putLong(value);
            position += 8;
        }
        processPosition();
        unmap(oldPosition, position);
    }


    /**
     *  Stores an eight-byte integer starting at the specified index.
     */
    public void putLong(long index, long value)
    {
        putLong(index, value, getByteOrder());
    }

    /**
     *  Stores an eight-byte integer starting at the specified index.
     */
    public void putLong(long index, long value, ByteOrder byteOrder)
    {
        long oldPosition = index;
        if(ByteUtils.NATIVE_BYTE_ORDER != byteOrder) {
            value = Long.reverseBytes(value);
        }
        NativeMappedMemory buffer = nativeMappedMemory(index);
        long remaining = buffer.remaining();
        if(remaining < 8) {
            switch((int)remaining) {
                case 1:
                    buffer.put(ByteUtils.long0(value));
                    index += 1;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 7;
                    break;
                case 2:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    index += 2;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 6;
                    break;
                case 3:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    index += 3;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 5;
                    break;
                case 4:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    index += 4;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 4;
                    break;
                case 5:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    index += 5;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 3;
                    break;
                case 6:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    index += 6;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long6(value));
                    buffer.put(ByteUtils.long7(value));
                    index += 2;
                    break;
                case 7:
                    buffer.put(ByteUtils.long0(value));
                    buffer.put(ByteUtils.long1(value));
                    buffer.put(ByteUtils.long2(value));
                    buffer.put(ByteUtils.long3(value));
                    buffer.put(ByteUtils.long4(value));
                    buffer.put(ByteUtils.long5(value));
                    buffer.put(ByteUtils.long6(value));
                    index += 7;
                    buffer = nativeMappedMemory(index);
                    buffer.put(ByteUtils.long7(value));
                    index += 1;
                    break;
            }
        }
        else {
            buffer.putLong(value);
            index += 8;
        }
        processPosition();
        unmap(oldPosition, index);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Float                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    @Override
    public float getFloat() {
        return getFloat(getByteOrder());
    }

    /**
     * Retrieves a four-byte integer starting at the current position.
     * @return
     */
    public float getFloat(ByteOrder byteOrder) {
        final int bits = getInt(byteOrder);
        float value = Float.intBitsToFloat(bits);
        return value;
    }


    /**
     *  Retrieves a four-byte floating-point number starting at the specified
     *  index.
     */
    public float getFloat(long index)
    {
        return getFloat(index, getByteOrder());
    }

    /**
     *  Retrieves a four-byte floating-point number starting at the specified
     *  index.
     */
    public float getFloat(long index, ByteOrder byteOrder)
    {
        final int bits = getInt(index, byteOrder);
        float value = Float.intBitsToFloat(bits);
        return value;
    }

    /**
     *  Stores a four-byte floating-point number starting at the current positon
     *  index.
     */
    public void putFloat(float value)
    {
        putFloat(value, getByteOrder());
    }

    /**
     *  Stores a four-byte floating-point number starting at the current positon
     *  index.
     */
    public void putFloat(float value, ByteOrder byteOrder)
    {
        final int intBits = Float.floatToIntBits(value);
        putInt(intBits, byteOrder);
    }

    /**
     *  Stores a four-byte floating-point number starting at the specified
     *  index.
     */
    public void putFloat(long index, float value)
    {
        putFloat(index, value, getByteOrder());
    }

    /**
     *  Stores a four-byte floating-point number starting at the specified
     *  index.
     */
    public void putFloat(long index, float value, ByteOrder byteOrder)
    {
        final int intBits = Float.floatToIntBits(value);
        putInt(index, intBits, byteOrder);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Double                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     *  Retrieves an eight-byte floating-point number starting at the specified
     *  index.
     */
    public double getDouble()
    {
        return getDouble(getByteOrder());
    }

    /**
     *  Retrieves an eight-byte floating-point number starting at the specified
     *  index.
     */
    public double getDouble(ByteOrder byteOrder)
    {
        final long bits = getLong(byteOrder);
        final double value = Double.longBitsToDouble(bits);
        return value;
    }

    /**
     *  Retrieves an eight-byte floating-point number starting at the specified
     *  index.
     */
    public double getDouble(long index)
    {
        return getDouble(index, getByteOrder());
    }

    /**
     *  Retrieves an eight-byte floating-point number starting at the specified
     *  index.
     */
    public double getDouble(long index, ByteOrder byteOrder)
    {
        final long bits = getLong(index, byteOrder);
        final double value = Double.longBitsToDouble(bits);
        return value;
    }

    /**
     *  Stores an eight-byte floating-point number starting at the current position
     */
    public void putDouble(double value)
    {
        putDouble(value, getByteOrder());
    }

    /**
     *  Stores an eight-byte floating-point number starting at the current position
     */
    public void putDouble(double value, ByteOrder byteOrder)
    {
        final long bits = Double.doubleToLongBits(value);
        putLong(bits, byteOrder);
    }

    /**
     *  Stores an eight-byte floating-point number starting at the specified
     *  index.
     */
    public void putDouble(long index, double value)
    {
        putDouble(index, value, getByteOrder());
    }

    /**
     *  Stores an eight-byte floating-point number starting at the specified
     *  index.
     */
    public void putDouble(long index, double value, ByteOrder byteOrder)
    {
        final long bits = Double.doubleToLongBits(value);
        putLong(index, bits, byteOrder);
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Char                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     *  Retrieves a two-byte character starting at the specified  index (note
     *  that a Unicode code point may require calling this method twice).
     */
    public char getChar(long index)
    {
        return buffer(index).getChar();
    }


    /**
     *  Stores a two-byte character starting at the specified  index.
     */
    public void putChar(long index, char value)
    {
        buffer(index).putChar(value);
    }


    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Bytes                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    /**
     *  Retrieves <code>len</code> bytes starting at the specified index,
     *  storing them in a newly created <code>byte[]</code>. Will span
     *  segments if necessary to retrieve the requested number of bytes.
     *
     *  @throws IndexOutOfBoundsException if the request would read past
     *          the end of file.
     */
    public byte[] getBytes(long index, int len)
    {
        byte[] ret = new byte[len];
        return getBytes(index, ret, 0, len);
    }


    /**
     *  Retrieves <code>len</code> bytes starting at the specified index,
     *  storing them in an existing <code>byte[]</code> at the specified
     *  offset. Returns the array as a convenience. Will span segments as
     *  needed.
     *
     *  @throws IndexOutOfBoundsException if the request would read past
     *          the end of file.
     */
    @Override
    public byte[] getBytes(long index, byte[] array, int off, int len)
    {
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            int count = Math.min(len, remainingAsInt(buf));
            final long bufferPosition = getBufferPosition(index);
            final int returnedCount = buf.getBytes(bufferPosition, array, off, count);
            assert (count == returnedCount);
            index += count;
            off += count;
            len -= count;
        }
        processPosition();
        return array;
    }

    public byte[] getBytes(byte[] array, int off, int len)
    {
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(position);
            int count = Math.min(len, remainingAsInt(buf));
            final long bufferPosition = getBufferPosition(position);
            final int returnedCount = buf.getBytes(bufferPosition, array, off, count);
            assert (count == returnedCount);
            position += count;
            off += count;
            len -= count;
        }
        processPosition();
        return array;
    }

    private int remainingAsInt(NativeMappedMemory buf) {
        return (int)Math.min(Integer.MAX_VALUE, buf.remaining());
    }

//    public ByteArraySlice getBytes(long index, ByteArraySlice destination, long offset, long length) {
//        destination.set
//
//        return destination;
//    }

    public ByteBuffer getBytes(ByteBuffer destination)
    {
        return getBytes(destination, destination.remaining());
    }

    /**
     *  Retrieves <code>len</code> bytes starting at the specified index,
     *  storing them in an existing <code>byte[]</code> at the specified
     *  offset. Returns the array as a convenience. Will span segments as
     *  needed.
     *
     *  @throws IndexOutOfBoundsException if the request would read past
     *          the end of file.
     */
    public ByteBuffer getBytes(ByteBuffer destination, int len)
    {
        while (len > 0)
        {
            currentByteBuffer = nativeMappedMemory(position);
            int count = Math.min(len, remainingAsInt(currentByteBuffer));
            final long bufferPosition = getBufferPosition(position);
            final int returnedCount = currentByteBuffer.getBytes(bufferPosition, destination, count);
            assert (count == returnedCount);
            position += count;
            len -= count;
        }
        processPosition();
        return destination;
    }

    public ByteBuffer getBytes(long index, ByteBuffer destination)
    {
        return getBytes(index, destination, destination.remaining());
    }

    /**
     *  Retrieves <code>len</code> bytes starting at the specified index,
     *  storing them in an existing <code>byte[]</code> at the specified
     *  offset. Returns the array as a convenience. Will span segments as
     *  needed.
     *
     *  @throws IndexOutOfBoundsException if the request would read past
     *          the end of file.
     */
    public ByteBuffer getBytes(long index, ByteBuffer destination, int len)
    {
        long initialPosition = index;
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            int count = Math.min(len, remainingAsInt(buf));
            final long bufferPosition = getBufferPosition(index);
            final int returnedCount = buf.getBytes(bufferPosition, destination, count);
            assert (count == returnedCount);
            index += count;
            len -= count;
        }
        processPosition();
        unmap(initialPosition, index);
        return destination;
    }

    public ByteBuffer putBytes(ByteBuffer source)
    {
        return putBytes(source, source.remaining());
    }

    public ByteBuffer putBytes(ByteBuffer source, int len)
    {
        while (len > 0)
        {
            currentByteBuffer = nativeMappedMemory(position);
            int count = Math.min(len, remainingAsInt(currentByteBuffer));
            final long bufferPosition = getBufferPosition(position);
            final int returnedCount = currentByteBuffer.putBytes(bufferPosition, source, count);
            assert (count == returnedCount);
            position += count;
            len -= count;
        }
        processPosition();
        return source;
    }

    public ByteBuffer putBytes(long index, ByteBuffer source) {
        return putBytes(index, source, source.remaining());
    }

    public ByteBuffer putBytes(long index, ByteBuffer source, int len)
    {
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            int count = Math.min(len, remainingAsInt(buf));
            final long bufferPosition = getBufferPosition(index);
            final int returnedCount = buf.putBytes(bufferPosition, source, count);
            assert (count == returnedCount);
            index += count;
            len -= count;
        }
        processPosition();
        return source;
    }


    /**
     *  Stores the contents of the passed byte array, starting at the given index.
     *  Will span segments as needed.
     *
     *  @throws IndexOutOfBoundsException if the request would write past
     *          the end of file.
     */
    public void putBytes(long index, byte[] value)
    {
        putBytes(index, value, 0, value.length);
    }


    /**
     *  Stores a section of the passed byte array, defined by <code>off</code> and
     *  <code>len</code>, starting at the given index. Will span segments as needed.
     *
     *  @throws IndexOutOfBoundsException if the request would write past
     *          the end of file.
     */
    @Override
    public void putBytes(long index, byte[] value, int off, int len)
    {
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            int count = Math.min(len, remainingAsInt(buf));
            final long bufferPosition = getBufferPosition(index);
            final int returnedCount = buf.putBytes(bufferPosition, value, off, count);
            assert (count == returnedCount);
            index += count;
            off += count;
            len -= count;
        }
        processPosition();
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* NativeMappedMemory                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public NativeMappedMemory getBytes(NativeMappedMemory destination)
    {
        final long destinationPosition = destination.position();
        final long length = destination.remaining();
        final NativeMappedMemory bytes = getBytes(destination, destinationPosition, length);
        destination.position(destinationPosition+length);
        return bytes;
    }

    public NativeMappedMemory getBytes(NativeMappedMemory destination, long length)
    {
        final long destinationPosition = destination.position();
        final NativeMappedMemory bytes = getBytes(destination, destinationPosition, length);
        destination.position(destinationPosition+length);
        return bytes;
    }

    /**
     *  Retrieves <code>len</code> bytes starting at the specified index,
     *  storing them in an existing <code>byte[]</code> at the specified
     *  offset. Returns the array as a convenience. Will span segments as
     *  needed.
     *
     *  @throws IndexOutOfBoundsException if the request would read past
     *          the end of file.
     */
    public NativeMappedMemory getBytes(NativeMappedMemory destination, long destinationPosition, long len)
    {
        while (len > 0)
        {
            currentByteBuffer = nativeMappedMemory(position);
            long count = Math.min(len, currentByteBuffer.remaining());
            final long bufferPosition = getBufferPosition(position);
            final long returnedCount = currentByteBuffer.getBytes(bufferPosition, destination, destinationPosition, count);
            assert (count == returnedCount);
            destinationPosition += count;
            position += count;
            len -= count;
        }
        processPosition();
        return destination;
    }

    public NativeMappedMemory getBytes(long index, NativeMappedMemory destination)
    {
        final long destinationPosition = destination.position();
        final long length = destination.remaining();
        final NativeMappedMemory bytes = getBytes(index, destination, destinationPosition, length);
        destination.position(destinationPosition+length);
        return bytes;
    }

    public NativeMappedMemory getBytes(long index, NativeMappedMemory destination, long length)
    {
        final long destinationPosition = destination.position();
        final NativeMappedMemory bytes = getBytes(index, destination, destinationPosition, length);
        destination.position(destinationPosition+length);
        return bytes;
    }

    public NativeMappedMemory getBytes(long index, NativeMappedMemory destination, long destinationPosition, long len)
    {
        long initialPosition = index;
        while (len > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            long count = Math.min(len, buf.remaining());
            final long bufferPosition = getBufferPosition(index);
            final long returnedCount = buf.getBytes(bufferPosition, destination, destinationPosition, count);
            assert (count == returnedCount);
            index += count;
            len -= count;
        }
        processPosition();
        unmap(initialPosition, index);
        return destination;
    }

    public NativeMappedMemory putBytes(NativeMappedMemory source)
    {
        final long sourcePosition = source.position();
        final long length = source.remaining();
        final NativeMappedMemory bytes = putBytes(source, sourcePosition, length);
        source.position(sourcePosition+length);
        return bytes;
    }

    public NativeMappedMemory putBytes(NativeMappedMemory source, long length)
    {
        final long sourcePosition = source.position();
        final NativeMappedMemory bytes = putBytes(source, sourcePosition, length);
        source.position(sourcePosition+length);
        return bytes;
    }

    public NativeMappedMemory putBytes(NativeMappedMemory source, long sourcePosition, long length)
    {
        while (length > 0)
        {
            currentByteBuffer = nativeMappedMemory(position);
            long count = Math.min(length, currentByteBuffer.remaining());
            final long bufferPosition = getBufferPosition(position);
            final long returnedCount = currentByteBuffer.putBytes(bufferPosition, source, sourcePosition, count);
            assert (count == returnedCount);
            sourcePosition += count;
            position += count;
            length -= count;
        }
        processPosition();
        return source;
    }

    public NativeMappedMemory putBytes(long index, NativeMappedMemory source)
    {
        final long sourcePosition = source.position();
        final long length = source.remaining();
        final NativeMappedMemory bytes = putBytes(index, source, sourcePosition, length);
        source.position(sourcePosition+length);
        return bytes;
    }

    public NativeMappedMemory putBytes(long index, NativeMappedMemory source, long length)
    {
        final long sourcePosition = source.position();
        final NativeMappedMemory bytes = putBytes(index, source, sourcePosition, length);
        source.position(sourcePosition+length);
        return bytes;
    }

    public NativeMappedMemory putBytes(long index, NativeMappedMemory source, long sourcePosition, long length)
    {
        while (length > 0)
        {
            NativeMappedMemory buf = nativeMappedMemory(index);
            long count = Math.min(length, buf.remaining());
            final long bufferPosition = getBufferPosition(index);
            final long returnedCount = buf.putBytes(bufferPosition, source, sourcePosition, count);
//            buf.force();
            assert (count == returnedCount);
            sourcePosition += count;
            index += count;
            length -= count;
        }
        processPosition();
        return source;
    }

    /**
     *  Creates a new buffer, whose size will be >= segment size, starting at
     *  the specified offset.
     */
    public ByteBuffer slice(long index)
    {
        return buffer(index).slice();
    }


    /**
     *  Iterates through the underlying buffers, calling <code>force()</code>
     *  on each; this will cause the buffers' contents to be written to disk.
     *  Note, however, that the OS may not physically write the buffers until
     *  a future time.
     */
    public void force()
    {
        for (NativeMappedMemory buf : _buffers) {
            if(buf.isMapped()) {
                buf.force();
            }
        }
    }


    /**
     * TODO implement
     *  Creates a new buffer referencing the same file, but with a copy of the
     *  original underlying mappings. The new and old buffers may be accessed
     *  by different threads.
     */
    @Override
    public NativeMappedFileBuffer clone()
    {
        try
        {
            NativeMappedFileBuffer that = (NativeMappedFileBuffer)super.clone();
            that._buffers = new NativeMappedMemory[_buffers.length];
            for (int ii = 0 ; ii < _buffers.length ; ii++)
            {
                // if the file is a multiple of the segment size, we
                // can end up with an empty slot in the buffer array
                if (_buffers[ii] != null) {
                    //TODO implement
//                    that._buffers[ii] = (NativeMappedMemory)_buffers[ii].duplicate();
                }
            }
            return that;
        }
        catch (CloneNotSupportedException ex)
        {
            throw new RuntimeException("I used to implement Cloneable, why don't I now?");
        }
    }

    public int getBuffersIndex(long filePosition) {
        return (int)(filePosition / _segmentSize);
    }

    /**
     * Wouldn't let me compile it unless i added this method
     * @return
     */
    @Override
    public byte[] getArray() {
        return new byte[0];
    }

    //----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    public NativeMappedMemory nativeMappedMemory(long filePosition)
    {
        long newCurrentPosition = getBufferPosition(filePosition);
        int bufferIndex = getBuffersIndex(filePosition);
        grow(bufferIndex);
        NativeMappedMemory buf = _buffers[bufferIndex];
        mapIfNecessary(buf, bufferIndex);
        buf.position(newCurrentPosition);
        assert (buf.remaining() != 0);
        assert(buf.order() == ByteOrder.nativeOrder());
        assert(buf.isMapped());
        return buf;
    }

    private NativeMappedMemory mapIfNecessary(NativeMappedMemory nativeMappedMemory, int bufferIndex) {
        if(!nativeMappedMemory.isMapped()) {
            try {
                long startPosition = bufferIndex*_segmentSize;
                MappedByteBuffer mappedByteBuffer = MapUtils.getMap(_channel, mapMode, startPosition, _segmentSize);
                if(!mappedByteBuffer.isLoaded()) {
                    mappedByteBuffer = mappedByteBuffer.load();
                }
                return nativeMappedMemory.wrap(mappedByteBuffer);
            }
            catch (IOException e) {
                long filePosition = bufferIndex * _segmentSize;
                throw new RuntimeException("Unable to remap at [filePosition="+filePosition+"][bufferIndex="+bufferIndex+"]", e);
            }
        }
        return nativeMappedMemory;
    }

//    private NativeMappedMemory mapSegment(final NativeMappedMemory nativeMappedMemory, int buffersIndex) throws IOException {
//        long startPosition = buffersIndex*_segmentSize;
//        final MappedByteBuffer mappedByteBuffer = MapUtils.getMap(_channel, mapMode, startPosition, _segmentSize);
//        mapIfNecessary(nativeMappedMemory, buffersIndex);
//        return nativeMappedMemory.wrap(mappedByteBuffer);
//    }

    // this is exposed for a white-box test of cloning
    public MappedByteBuffer buffer(long filePosition)
    {
        return nativeMappedMemory(filePosition).buffer();
//        long newCurrentPosition = getBufferPosition(filePosition);
//        int bufferIndex = getBuffersIndex(filePosition);
//        grow(bufferIndex);
//        MappedByteBuffer buf = _buffers[bufferIndex].buffer();
//        buf.position((int)newCurrentPosition);
//        assert (buf.remaining() != 0);
//        assert(buf.order() == ByteOrder.nativeOrder());
//        return buf;
    }

    private void unmap(long oldPosition, long newPosition) {
        int oldIndex = getBuffersIndex(oldPosition);
        int newIndex = getBuffersIndex(newPosition);
        for(int i=oldIndex; i<newIndex; i++) {
            unmap(_buffers[i]);
        }
    }

    private static void unmap(NativeMappedMemory nativeMappedMemory) {
        if((nativeMappedMemory != null) && nativeMappedMemory.isMapped()) {
            nativeMappedMemory.force();
            final boolean released = nativeMappedMemory.release();
            assert(released == true) : "Unable to release " + nativeMappedMemory;
        }
    }
}
