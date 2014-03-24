/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.binaryfile;


import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.lang.io.MappedFile;
import net.openhft.lang.io.MappedMemory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class BinaryFile
{
    private static final Logger log = LogManager.getLogger(BinaryFile.class);

    private final String filePath;
    private final MappedFile mappedFile;
    private final int blockSize;
//    private final int overlapSize;
    private final DirectBuffer directBuffer;
    private long sequence = 0;
    private long currentFilePosition = 0;

    private short nextMessageLength = 0;
    private long nextBufferPosition = 0;


    public BinaryFile(String filePath, int blockSize) throws IOException {
        this.filePath = filePath;
        this.currentFilePosition = 0;
        this.blockSize = blockSize;
        this.nextBufferPosition += this.blockSize;
        this.mappedFile = new MappedFile(filePath, blockSize);
        MappedMemory mappedMemory = this.mappedFile.acquire(currentFilePosition);
        MappedByteBuffer buffer = mappedMemory.buffer();
        this.directBuffer = new DirectBuffer(buffer);

        this.nextMessageLength = getNextMessageLength();
    }


    @Override
    public String toString() {
        return "BinaryFile{" +
                "filePath='" + filePath + '\'' +
                ", blockSize=" + blockSize +
                ", sequence=" + sequence +
                ", currentFilePosition=" + currentFilePosition +
                ", nextMessageLength=" + nextMessageLength +
                ", nextBufferPosition=" + nextBufferPosition +
                '}';
    }

    public boolean hasNext() {
        return (nextMessageLength != 0);
    }

    public void next(ByteBuffer destination) throws IOException {
        final int remaining = destination.remaining();
        if(remaining < nextMessageLength) {
            throw new RuntimeException("Not enough room for nextMessageLength of " + nextMessageLength + " and ByteBuffer remaining is only " + remaining);
        }
        int directBufferIndex = getDirectBufferIndex(currentFilePosition);
        final int bytesLength = this.directBuffer.getBytes(directBufferIndex, destination, nextMessageLength);
        assert(bytesLength == nextMessageLength);
        this.sequence++;
        this.currentFilePosition += nextMessageLength;
        checkDirectBuffer();
        this.nextMessageLength = getNextMessageLength();

    }

    private void checkDirectBuffer() throws IOException {
        if(this.currentFilePosition >= nextBufferPosition) {
            MappedMemory mappedMemory = this.mappedFile.acquire(currentFilePosition);
            MappedByteBuffer buffer = mappedMemory.buffer();
            this.directBuffer.wrap(buffer);
            nextBufferPosition += blockSize;
        }
    }

    public long getSequence() {
        return sequence;
    }

    public short getNextMessageLength() throws IOException {
        int directBufferIndex = getDirectBufferIndex(currentFilePosition);
        final short messageLength = this.directBuffer.getShort(directBufferIndex, ByteOrder.BIG_ENDIAN);
        this.currentFilePosition += BitUtil.SIZE_OF_SHORT;
        checkDirectBuffer();
        return messageLength;
    }

    private int getDirectBufferIndex(long position) {
        int directBufferIndex = (int)(position % blockSize);
        return directBufferIndex;
    }

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystems.getDefault();
        Path path = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        String absolutePath = file.getAbsolutePath();
        int dataBlockSize = ChronicleConfig.TEST.dataBlockSize();

        final ByteBuffer tempByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE);

        BinaryFile binaryFile = new BinaryFile(absolutePath, dataBlockSize);
        long binaryFileSequence = -1;
        while(binaryFile.hasNext()) {
            tempByteBuffer.clear();
            binaryFile.next(tempByteBuffer);
            tempByteBuffer.flip();
            final byte messageType = tempByteBuffer.get();
            final ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
            if(itchMessageType == ITCHMessageType.NULL_VAL) {
                throw new RuntimeException("Null ITCHMessageType for " + binaryFile);
            }
            binaryFileSequence = binaryFile.getSequence();
            if((binaryFileSequence < 10) || (binaryFileSequence % 100 == 0)) {
                log.info("end - [sequence=" + binaryFile.getSequence() + "][itchMessageType="+itchMessageType+"]");
            }
        }
        log.info("end - [sequence=" + binaryFileSequence + "]");
    }
}
