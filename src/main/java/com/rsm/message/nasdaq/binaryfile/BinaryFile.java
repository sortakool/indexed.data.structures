/* Generated SBE (Simple Binary Encoding) message codec */
package com.rsm.message.nasdaq.binaryfile;


import com.rsm.buffer.NativeMappedFileBuffer;
import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUnit;
import com.rsm.util.ByteUtils;
import net.openhft.chronicle.ChronicleConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class BinaryFile
{
    private static final Logger log = LogManager.getLogger(BinaryFile.class);

    private final String filePath;
    private final Path path;
    private final File file;
    final NativeMappedFileBuffer mappedFile;
    private final long blockSize;
    private final long initialFileSize;
    private final ByteOrder byteOrder;
    private long sequence = 0;
    private long currentFilePosition = 0;

    private short nextMessageLength = 0;
    private long nextBufferPosition = 0;


    public BinaryFile(String filePath, long blockSize, long initialFileSize, ByteOrder byteOrder) throws IOException {
        this.filePath = filePath;
        this.currentFilePosition = 0;
        this.blockSize = blockSize;
        this.initialFileSize = initialFileSize;
        this.byteOrder = byteOrder;
        this.nextBufferPosition += this.blockSize;
        path = Paths.get(filePath);
        file = path.toFile();
        mappedFile = new NativeMappedFileBuffer(file, this.blockSize, this.initialFileSize, true, false);
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

    public short next(ByteBuffer destination) throws IOException {
        short currentMessageLength = nextMessageLength;
        this.mappedFile.getBytes(currentFilePosition, destination, currentMessageLength);
        this.sequence++;
        this.currentFilePosition += currentMessageLength;
        this.nextMessageLength = getNextMessageLength();
        return currentMessageLength;

    }

    public long getSequence() {
        return sequence;
    }

    public short getCurrentMessageLength() {
        return nextMessageLength;
    }

    public short getNextMessageLength() throws IOException {
        final short messageLength = this.mappedFile.getShort(currentFilePosition, byteOrder);
        this.currentFilePosition += BitUtil.SIZE_OF_SHORT;
        return messageLength;
    }

//    private int getDirectBufferIndex(long position) {
//        int directBufferIndex = (int)(position % blockSize);
//        return directBufferIndex;
//    }

    public void reset() throws IOException {
        currentFilePosition = 0;
        nextMessageLength = getNextMessageLength();
    }

    public long length() {
        return mappedFile.capacity();
    }

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystems.getDefault();
        Path path = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        long fileSize = file.length()*2;
        String absolutePath = file.getAbsolutePath();
//        long dataBlockSize = ByteUtils.PAGE_SIZE*10;
        long dataBlockSize = 1000*1000*32;//force page aligned segment size

        final ByteBuffer tempByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE);

        final long startTime = System.nanoTime();
        BinaryFile binaryFile = new BinaryFile(absolutePath, dataBlockSize, fileSize, ByteOrder.BIG_ENDIAN);
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
            if((binaryFileSequence < 10) || (binaryFileSequence % 1_000_000 == 0)) {
                log.info("[sequence=" + binaryFile.getSequence() + "][itchMessageType="+itchMessageType+"]");
            }
        }
        final long endTime = System.nanoTime();
        long duration = endTime - startTime;
        log.info("end - [sequence=" + binaryFileSequence + "][duration="+ TimeUnit.NANOSECONDS.toMillis(duration)+"ms]");
    }
}
