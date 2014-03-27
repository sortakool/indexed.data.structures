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

import com.rsm.byteSlice.ByteArraySlice;
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
import java.util.Arrays;


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
 */
public class MappedFileBuffer
implements BufferFacade, Cloneable
{

    private static final Logger log = LogManager.getLogger(MappedFileBuffer.class);

    public final static int MAX_SEGMENT_SIZE = 0x8000000; // 1 GB, assures alignment

    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    private File _file;
    private boolean _isWritable;
    private long _initialFileSize;
    private long _segmentSize;              // long because it's used in long expressions
    private long _growBySize;
    private MappedByteBuffer[] _buffers;
    private long position = 0;
    private MappedByteBuffer currentByteBuffer;
    private int currentIndex;
    private RandomAccessFile _mappedFile;
    private FileChannel _channel;

    public void print(StringBuilder sb) {
        int bufferPosition = getBufferPosition(position);
        int buffersLength = 0;
        if(_buffers != null) {
            buffersLength =  _buffers.length;
        }
    }

    public int getBufferPosition(long position) {
        return (int)(position % _segmentSize);
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
    public MappedFileBuffer(File file)
    throws IOException
    {
        this(file, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE, true, false);
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
    public MappedFileBuffer(File file, boolean readWrite)
    throws IOException
    {
        this(file, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE, readWrite, false);
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
    public MappedFileBuffer(File file, int segmentSize, long initialFileSize, long growBySize, boolean readWrite, boolean deleteFileIfExists)
    throws IOException
    {
        if (segmentSize > MAX_SEGMENT_SIZE)
            throw new IllegalArgumentException(
                    "segment size too large (max is " + MAX_SEGMENT_SIZE + "): " + segmentSize);

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
        _segmentSize = segmentSize;
        _initialFileSize = initialFileSize;
        _growBySize = growBySize;

        try
        {
            String mode = readWrite ? "rw" : "r";
            MapMode mapMode = readWrite ? MapMode.READ_WRITE : MapMode.READ_ONLY;

            _mappedFile = new RandomAccessFile(file, mode);
            _channel = _mappedFile.getChannel();

            //make each segment equal sizes
            long fileSize = Math.max(_segmentSize, capacity());
            long remainder = fileSize % _segmentSize;
            fileSize = _initialFileSize + remainder;

            int bufArraySize = (int)(fileSize / segmentSize)
                             + ((fileSize % segmentSize != 0) ? 1 : 0);
            _buffers = new MappedByteBuffer[bufArraySize];
            int bufIdx = 0;
            for (long offset = 0 ; offset < fileSize ; offset += segmentSize)
            {
                long remainingFileSize = fileSize - offset;
                long thisSegmentSize = Math.min(2L * segmentSize, remainingFileSize);
                _buffers[bufIdx++] = _channel.map(mapMode, offset, thisSegmentSize);
            }
            position = 0;
            currentByteBuffer = buffer(position);
            currentIndex = getBuffersIndex(position);
        }
        finally
        {
//            IOUtil.closeQuietly(mappedFile);
        }
    }

    private void grow(int index) {
        if(index >= _buffers.length) {
            long capacity = capacity();
            long fileSize = capacity() + _growBySize;
            long remainder = fileSize % _segmentSize;
            fileSize += remainder;

            MapMode mapMode = _isWritable ? MapMode.READ_WRITE : MapMode.READ_ONLY;

            int bufArraySize = (int)(fileSize / _segmentSize)
                    + ((fileSize % _segmentSize != 0) ? 1 : 0);
            log.info("growing buffers array size from " + _buffers.length + " to " + bufArraySize);
            MappedByteBuffer[] temp = new MappedByteBuffer[bufArraySize];
//            long remainingFileSize = fileSize - offset;
//            long thisSegmentSize = Math.min(2L * _segmentSize, remainingFileSize);
            int bufIdx = 0;
            for (long offset = 0 ; offset < fileSize ; offset += _segmentSize)
            {
                if(bufIdx < _buffers.length) {
                    try {
                        map(mapMode, temp, bufIdx, offset, _segmentSize);
                        _buffers[bufIdx].clear();
                        temp[bufIdx].put(_buffers[bufIdx]);
                    }
                    catch(Exception e) {
                        log.warn("Unable to memory-map file at index " + bufIdx, e);
                    }
                }
                else {
                    map(mapMode, temp, bufIdx, offset, _segmentSize);
                }
                bufIdx++;
            }
            _buffers = temp;
        }
    }

     private void map(MapMode mapMode, MappedByteBuffer[] mappedByteBuffer, int bufIdx, long offset, long thisSegmentSize) {
         try {
             mappedByteBuffer[bufIdx] = MapUtils.getMap(_channel, mapMode, offset, thisSegmentSize);
         }
         catch(IOException e) {
             log.warn("Unable to memory-map file at index " + bufIdx, e);
         }
     }

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
                            Thread.currentThread().interrupt();;
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

    public long position() {
        return position;
    }

    public MappedFileBuffer position(long position) {
        this.position = position;
        currentByteBuffer = buffer(position);
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
     *  the {@link BufferFacade} interface.
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
        return _buffers[0].order();
    }


    /**
     *  Sets the order of this buffer (propagated to all child buffers).
     */
    public void setByteOrder(ByteOrder order)
    {
        for (ByteBuffer child : _buffers)
            child.order(order);
    }

    @Override
    public int getCurrentIndex() {
        return currentIndex;
    }

    /**
     *  Retrieves a single byte from the current position.
     */
    @Override
    public byte get()
    {
        byte value = currentByteBuffer.get();
        position += 1;
        currentByteBuffer = buffer(position);
        return value;
    }

    public byte get(long index) {
        return buffer(index).get();
    }

    @Override
    public void put(byte value) {
        currentByteBuffer.put(value);
        position += 1;
        currentByteBuffer = buffer(position);
    }

    /**
     *  Stores a single byte at the specified index.
     */
    public void put(long index, byte value)
    {
        buffer(index).put(value);
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
        short value = 0;
        final int remaining = currentByteBuffer.remaining();
        if (remaining < 2) {
            byte short0 = 0;
            byte short1 = 0;
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        short1 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
                        short0 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            } else {      //ByteOrder.LITTLE_ENDIAN
                switch (remaining) {
                    case 1:
                        short0 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
                        short1 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            }
            currentByteBuffer = buffer(position);
            value = ByteUtils.makeShort(short1, short0);
        }
        else {
            value = currentByteBuffer.getShort();
            if(getByteOrder() != byteOrder) {
                value = Short.reverseBytes(value);
            }
            position += 2;
            currentByteBuffer = buffer(position);
        }
        currentIndex = getBuffersIndex(position);
        return value;
    }

    /**
     * Retrieves a two-byte short starting at the specified position.
     * @return
     */
    public short getShort(long index, ByteOrder byteOrder) {
        short value = 0;
        MappedByteBuffer buffer = buffer(index);
        final int remaining = buffer.remaining();
        if (remaining < 2) {
            byte short0 = 0;
            byte short1 = 0;
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        short1 = buffer.get();
                        index += 1;
                        buffer = buffer(index);
                        short0 = buffer.get();
                        index += 1;
                        break;
                }
            }
            else {      //ByteOrder.LITTLE_ENDIAN
                switch (remaining) {
                    case 1:
                        short0 = buffer.get();
                        index += 1;
                        buffer = buffer(index);
                        short1 = buffer.get();
                        index += 1;
                        break;
                }
            }
            value = ByteUtils.makeShort(short1, short0);
        }
        else {
            value = buffer.getShort();
            if(getByteOrder() != byteOrder) {
                value = Short.reverseBytes(value);
            }
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
        final MappedByteBuffer buffer = buffer(index);
        int remaining = buffer.remaining();
        if(remaining < 2) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        buffer.put(ByteUtils.short1(value));
                        index += 1;
                        buffer.put(ByteUtils.short0(value));
                        index += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        buffer.put(ByteUtils.short0(value));
                        index += 1;
                        buffer.put(ByteUtils.short1(value));
                        index += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Short.reverseBytes(value);
            }
            buffer(index).putShort(value);
        }
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
        int remaining = currentByteBuffer.remaining();
        if(remaining < 2) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.short1(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.short0(value));
                        position += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.short0(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.short1(value));
                        position += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Short.reverseBytes(value);
            }
            currentByteBuffer.putShort(value);
            position += 2;
        }
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = buffer(position);
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
        int value = 0;
        int remaining = currentByteBuffer.remaining();
        if(remaining < 4) {
            byte int0 = 0;
            byte int1 = 0;
            byte int2 = 0;
            byte int3 = 0;
            if(getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        int3 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
                        int2 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        int0 = currentByteBuffer.get();
                        position += 3;
                        break;
                    case 2:
                        int3 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        position += 2;
                        currentByteBuffer = buffer(position);
                        int1 = currentByteBuffer.get();
                        int0 = currentByteBuffer.get();
                        position += 2;
                        break;
                    case 3:
                        int3 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        position += 3;
                        currentByteBuffer = buffer(position);
                        int0 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            }
            else {      //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        int0 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
                        int1 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        int3 = currentByteBuffer.get();
                        position += 3;
                        break;
                    case 2:
                        int0 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        position += 2;
                        currentByteBuffer = buffer(position);
                        int2 = currentByteBuffer.get();
                        int3 = currentByteBuffer.get();
                        position += 2;
                        break;
                    case 3:
                        int0 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        position += 3;
                        currentByteBuffer = buffer(position);
                        int3 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            }
            value = ByteUtils.makeInt(int3, int2, int1, int0);
        }
        else {
            value = currentByteBuffer.getInt();
            if(getByteOrder() != byteOrder) {
                value = Integer.reverseBytes(value);
            }
            position += 4;
        }
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = buffer(position);
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
        int value = 0;
        MappedByteBuffer buffer = buffer(index);
        int remaining = buffer.remaining();
        if(remaining < 4) {
            byte int0 = 0;
            byte int1 = 0;
            byte int2 = 0;
            byte int3 = 0;
            if(getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        int3 = buffer.get();
                        index += 1;
                        buffer = buffer(index);
                        int2 = buffer.get();
                        int1 = buffer.get();
                        int0 = buffer.get();
                        index += 3;
                        break;
                    case 2:
                        int3 = buffer.get();
                        int2 = buffer.get();
                        index += 2;
                        buffer = buffer(index);
                        int1 = buffer.get();
                        int0 = buffer.get();
                        index += 2;
                        break;
                    case 3:
                        int3 = buffer.get();
                        int2 = buffer.get();
                        int1 = buffer.get();
                        index += 3;
                        buffer = buffer(index);
                        int0 = buffer.get();
                        index += 1;
                        break;
                }
            }
            else {      //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        int0 = currentByteBuffer.get();
                        index += 1;
                        buffer = buffer(index);
                        int1 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        int3 = currentByteBuffer.get();
                        index += 3;
                        break;
                    case 2:
                        int0 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        index += 2;
                        buffer = buffer(index);
                        int2 = currentByteBuffer.get();
                        int3 = currentByteBuffer.get();
                        index += 2;
                        break;
                    case 3:
                        int0 = currentByteBuffer.get();
                        int1 = currentByteBuffer.get();
                        int2 = currentByteBuffer.get();
                        index += 3;
                        buffer = buffer(index);
                        int3 = currentByteBuffer.get();
                        index += 1;
                        break;
                }
            }
            value = ByteUtils.makeInt(int3, int2, int1, int0);
        }
        else {
            value = buffer.getInt();
            if(getByteOrder() != byteOrder) {
                value = Integer.reverseBytes(value);
            }
//            index += 4;
            buffer = buffer(index);
        }
//        index = getBuffersIndex(index);
        return value;
    }

    @Override
    public void putInt(int value) {
        putInt(value, getByteOrder());
    }

    public void putInt(int value, ByteOrder byteOrder) {
        int remaining = currentByteBuffer.remaining();
        if(remaining < 4) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.int3(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int2(value));
                        currentByteBuffer.put(ByteUtils.int1(value));
                        currentByteBuffer.put(ByteUtils.int0(value));
                        position += 3;
                        break;
                    case 2:
                        currentByteBuffer.put(ByteUtils.int3(value));
                        currentByteBuffer.put(ByteUtils.int2(value));
                        position += 2;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int1(value));
                        currentByteBuffer.put(ByteUtils.int0(value));
                        position += 2;
                        break;
                    case 3:
                        currentByteBuffer.put(ByteUtils.int3(value));
                        currentByteBuffer.put(ByteUtils.int2(value));
                        currentByteBuffer.put(ByteUtils.int1(value));
                        position += 3;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int0(value));
                        position += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.int0(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int1(value));
                        currentByteBuffer.put(ByteUtils.int2(value));
                        currentByteBuffer.put(ByteUtils.int3(value));
                        position += 3;
                        break;
                    case 2:
                        currentByteBuffer.put(ByteUtils.int0(value));
                        currentByteBuffer.put(ByteUtils.int1(value));
                        position += 2;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int2(value));
                        currentByteBuffer.put(ByteUtils.int3(value));
                        position += 2;
                        break;
                    case 3:
                        currentByteBuffer.put(ByteUtils.int0(value));
                        currentByteBuffer.put(ByteUtils.int1(value));
                        currentByteBuffer.put(ByteUtils.int2(value));
                        position += 3;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.int3(value));
                        position += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Integer.reverseBytes(value);
            }
            currentByteBuffer.putInt(value);
            position += 4;
        }
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = buffer(position);
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
        ByteBuffer buffer = buffer(index);
        int remaining = buffer.remaining();
        if(remaining < 4) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        buffer.put(ByteUtils.int3(value));
                        index += 1;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int2(value));
                        buffer.put(ByteUtils.int1(value));
                        buffer.put(ByteUtils.int0(value));
                        index += 3;
                        break;
                    case 2:
                        buffer.put(ByteUtils.int3(value));
                        buffer.put(ByteUtils.int2(value));
                        index += 2;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int1(value));
                        buffer.put(ByteUtils.int0(value));
                        index += 2;
                        break;
                    case 3:
                        buffer.put(ByteUtils.int3(value));
                        buffer.put(ByteUtils.int2(value));
                        buffer.put(ByteUtils.int1(value));
                        index += 3;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int0(value));
                        index += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        buffer.put(ByteUtils.int0(value));
                        index += 1;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int1(value));
                        buffer.put(ByteUtils.int2(value));
                        buffer.put(ByteUtils.int3(value));
                        index += 3;
                        break;
                    case 2:
                        buffer.put(ByteUtils.int0(value));
                        buffer.put(ByteUtils.int1(value));
                        index += 2;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int2(value));
                        buffer.put(ByteUtils.int3(value));
                        index += 2;
                        break;
                    case 3:
                        buffer.put(ByteUtils.int0(value));
                        buffer.put(ByteUtils.int1(value));
                        buffer.put(ByteUtils.int2(value));
                        index += 3;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.int3(value));
                        index += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Integer.reverseBytes(value);
            }
            buffer.putInt(value);
        }
    }

    /* ----------------------------------------------------------------------------------------------------------------------------- */
    /* Long                                                                                                                         */
    /* ----------------------------------------------------------------------------------------------------------------------------- */

    public long  getLong() {
        return getLong(getByteOrder());
    }

    public long  getLong(ByteOrder byteOrder) {
        long value = 0;
        int remaining = currentByteBuffer.remaining();
        if(remaining < 8) {
            byte long0 = 0;
            byte long1 = 0;
            byte long2 = 0;
            byte long3 = 0;
            byte long4 = 0;
            byte long5 = 0;
            byte long6 = 0;
            byte long7 = 0;
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        long7 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 7;
                        break;
                    case 2:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        position += 2;
                        currentByteBuffer = buffer(position);
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 6;
                        break;
                    case 3:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        position += 3;
                        currentByteBuffer = buffer(position);
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 5;
                        break;
                    case 4:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        position += 4;
                        currentByteBuffer = buffer(position);
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 4;
                        break;
                    case 5:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        position += 5;
                        currentByteBuffer = buffer(position);
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 3;
                        break;
                    case 6:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        position += 6;
                        currentByteBuffer = buffer(position);
                        long1 = currentByteBuffer.get();
                        long0 = currentByteBuffer.get();
                        position += 2;
                        break;
                    case 7:
                        long7 = currentByteBuffer.get();
                        long6 = currentByteBuffer.get();
                        long5 = currentByteBuffer.get();
                        long4 = currentByteBuffer.get();
                        long3 = currentByteBuffer.get();
                        long2 = currentByteBuffer.get();
                        long1 = currentByteBuffer.get();
                        position += 7;
                        currentByteBuffer = buffer(position);
                        long0 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            }
            else {      //ByteOrder.LITTLE_ENDIAN
                switch (remaining) {
                    case 1:
                        long0 = currentByteBuffer.get();
                        position += 1;
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
                        long7 = currentByteBuffer.get();
                        position += 1;
                        break;
                }
            }
            value = ByteUtils.makeLong(long7, long6, long5, long4, long3, long2, long1, long0);
        }
        else {
            value = currentByteBuffer.getLong();
            if(getByteOrder() != byteOrder) {
                value = Long.reverseBytes(value);
            }
        }
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = buffer(position);
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
        long value = 0;
        MappedByteBuffer buffer = buffer(index);
        int remaining = buffer.remaining();
        if(remaining < 8) {
            byte long0 = 0;
            byte long1 = 0;
            byte long2 = 0;
            byte long3 = 0;
            byte long4 = 0;
            byte long5 = 0;
            byte long6 = 0;
            byte long7 = 0;
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch (remaining) {
                    case 1:
                        long7 = buffer.get();
                        index += 1;
                        buffer = buffer(index);
                        long6 = buffer.get();
                        long5 = buffer.get();
                        long4 = buffer.get();
                        long3 = buffer.get();
                        long2 = buffer.get();
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 7;
                        break;
                    case 2:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        index += 2;
                        buffer = buffer(index);
                        long5 = buffer.get();
                        long4 = buffer.get();
                        long3 = buffer.get();
                        long2 = buffer.get();
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 6;
                        break;
                    case 3:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        long5 = buffer.get();
                        index += 3;
                        buffer = buffer(index);
                        long4 = buffer.get();
                        long3 = buffer.get();
                        long2 = buffer.get();
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 5;
                        break;
                    case 4:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        long5 = buffer.get();
                        long4 = buffer.get();
                        index += 4;
                        buffer = buffer(index);
                        long3 = buffer.get();
                        long2 = buffer.get();
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 4;
                        break;
                    case 5:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        long5 = buffer.get();
                        long4 = buffer.get();
                        long3 = buffer.get();
                        index += 5;
                        buffer = buffer(index);
                        long2 = buffer.get();
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 3;
                        break;
                    case 6:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        long5 = buffer.get();
                        long4 = buffer.get();
                        long3 = buffer.get();
                        long2 = buffer.get();
                        index += 6;
                        buffer = buffer(index);
                        long1 = buffer.get();
                        long0 = buffer.get();
                        index += 2;
                        break;
                    case 7:
                        long7 = buffer.get();
                        long6 = buffer.get();
                        long5 = buffer.get();
                        long4 = buffer.get();
                        long3 = buffer.get();
                        long2 = buffer.get();
                        long1 = buffer.get();
                        index += 7;
                        buffer = buffer(index);
                        long0 = buffer.get();
                        index += 1;
                        break;
                }
            }
            else {      //ByteOrder.LITTLE_ENDIAN
                switch (remaining) {
                    case 1:
                        long0 = buffer.get();
                        index += 1;
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
                        long7 = buffer.get();
                        index += 1;
                        break;
                }
            }
            value = ByteUtils.makeLong(long7, long6, long5, long4, long3, long2, long1, long0);
        }
        else {
            value = buffer.getLong();
            if(getByteOrder() != byteOrder) {
                value = Long.reverseBytes(value);
            }
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
        int remaining = currentByteBuffer.remaining();
        if(remaining < 8) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 7;
                        break;
                    case 2:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        position += 2;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 6;
                        break;
                    case 3:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        position += 3;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 5;
                        break;
                    case 4:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        position += 4;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 4;
                        break;
                    case 5:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        position += 5;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 3;
                        break;
                    case 6:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        position += 6;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long1(value));
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 2;
                        break;
                    case 7:
                        currentByteBuffer.put(ByteUtils.long7(value));
                        currentByteBuffer.put(ByteUtils.long6(value));
                        currentByteBuffer.put(ByteUtils.long5(value));
                        currentByteBuffer.put(ByteUtils.long4(value));
                        currentByteBuffer.put(ByteUtils.long3(value));
                        currentByteBuffer.put(ByteUtils.long2(value));
                        currentByteBuffer.put(ByteUtils.long1(value));
                        position += 7;
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        currentByteBuffer.put(ByteUtils.long0(value));
                        position += 1;
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
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
                        currentByteBuffer = buffer(position);
                        currentByteBuffer.put(ByteUtils.long7(value));
                        position += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Long.reverseBytes(value);
            }
            currentByteBuffer.putLong(value);
            position += 8;
        }
        currentIndex = getBuffersIndex(position);
        currentByteBuffer = buffer(position);
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
        ByteBuffer buffer = buffer(index);
        int remaining = buffer.remaining();
        if(remaining < 8) {
            if (getByteOrder() == ByteOrder.BIG_ENDIAN) {
                switch(remaining) {
                    case 1:
                        buffer.put(ByteUtils.long7(value));
                        index += 1;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 7;
                        break;
                    case 2:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        index += 2;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 6;
                        break;
                    case 3:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        index += 3;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 5;
                        break;
                    case 4:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        index += 4;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 4;
                        break;
                    case 5:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        index += 5;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 3;
                        break;
                    case 6:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        index += 6;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long1(value));
                        buffer.put(ByteUtils.long0(value));
                        index += 2;
                        break;
                    case 7:
                        buffer.put(ByteUtils.long7(value));
                        buffer.put(ByteUtils.long6(value));
                        buffer.put(ByteUtils.long5(value));
                        buffer.put(ByteUtils.long4(value));
                        buffer.put(ByteUtils.long3(value));
                        buffer.put(ByteUtils.long2(value));
                        buffer.put(ByteUtils.long1(value));
                        index += 7;
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long0(value));
                        index += 1;
                        break;
                }
            }
            else { //ByteOrder.LITTLE_ENDIAN
                switch(remaining) {
                    case 1:
                        buffer.put(ByteUtils.long0(value));
                        index += 1;
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
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
                        buffer = buffer(index);
                        buffer.put(ByteUtils.long7(value));
                        index += 1;
                        break;
                }
            }
        }
        else {
            if(getByteOrder() != byteOrder) {
                value = Long.reverseBytes(value);
            }
            buffer.putLong(value);
        }
    }




    /**
     *  Retrieves a four-byte floating-point number starting at the specified
     *  index.
     */
    public float getFloat(long index)
    {
        return buffer(index).getFloat();
    }


    /**
     *  Stores a four-byte floating-point number starting at the specified
     *  index.
     */
    public void putFloat(long index, float value)
    {
        buffer(index).putFloat(value);
    }


    /**
     *  Retrieves an eight-byte floating-point number starting at the specified
     *  index.
     */
    public double getDouble(long index)
    {
        return buffer(index).getDouble();
    }


    /**
     *  Stores an eight-byte floating-point number starting at the specified
     *  index.
     */
    public void putDouble(long index, double value)
    {
        buffer(index).putDouble(value);
    }


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
            ByteBuffer buf = buffer(index);
            int count = Math.min(len, buf.remaining());
            buf.get(array, off, count);
            index += count;
            off += count;
            len -= count;
        }
        return array;
    }

    public byte[] getBytes(byte[] array, int off, int len)
    {
        while (len > 0)
        {
            ByteBuffer buf = currentByteBuffer;
            int count = Math.min(len, buf.remaining());
            buf.get(array, off, count);
            off += count;
            len -= count;
        }
        return array;
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
            currentByteBuffer = buffer(position);
            int count = Math.min(len, currentByteBuffer.remaining());
//            buf.put(destination);
//            destination.put(buf);
            for(int i=0; i< count; i++) {
                destination.put(currentByteBuffer.get());
            }
            position += count;
            currentByteBuffer = buffer(position);
            len -= count;
        }
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
        while (len > 0)
        {
            ByteBuffer buf = buffer(index);
            int count = Math.min(len, buf.remaining());
//            buf.put(destination);
//            destination.put(buf);
            for(int i=0; i< count; i++) {
                destination.put(buf.get());
            }
            index += count;
            len -= count;
        }
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
            currentByteBuffer = buffer(position);
            int count = Math.min(len, currentByteBuffer.remaining());
            for(int i=0; i< count; i++) {
                currentByteBuffer.put(source.get());
            }
            position += count;
            currentByteBuffer = buffer(position);
            len -= count;
        }
        return source;
    }

    public ByteBuffer putBytes(long index, ByteBuffer source) {
        return putBytes(index, source, source.remaining());
    }

    public ByteBuffer putBytes(long index, ByteBuffer source, int len)
    {
        while (len > 0)
        {
            ByteBuffer buf = buffer(index);
            int count = Math.min(len, buf.remaining());
            for(int i=0; i< count; i++) {
                buf.put(source.get());
            }
            index += count;
            len -= count;
        }
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
            ByteBuffer buf = buffer(index);
            int count = Math.min(len, buf.remaining());
            buf.put(value, off, count);
            index += count;
            off += count;
            len -= count;
        }
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
        for (MappedByteBuffer buf : _buffers)
            buf.force();
    }


    /**
     *  Creates a new buffer referencing the same file, but with a copy of the
     *  original underlying mappings. The new and old buffers may be accessed
     *  by different threads.
     */
    @Override
    public MappedFileBuffer clone()
    {
        try
        {
            MappedFileBuffer that = (MappedFileBuffer)super.clone();
            that._buffers = new MappedByteBuffer[_buffers.length];
            for (int ii = 0 ; ii < _buffers.length ; ii++)
            {
                // if the file is a multiple of the segment size, we
                // can end up with an empty slot in the buffer array
                if (_buffers[ii] != null)
                    that._buffers[ii] = (MappedByteBuffer)_buffers[ii].duplicate();
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

    // this is exposed for a white-box test of cloning
    public MappedByteBuffer buffer(long filePosition)
    {
        int newCurrentPosition = getBufferPosition(filePosition);
        int bufferIndex = getBuffersIndex(filePosition);
        grow(bufferIndex);
        MappedByteBuffer buf = _buffers[bufferIndex];
        buf.position(newCurrentPosition);
        assert (buf.remaining() != 0);
        return buf;
    }
}
