package com.rsm.message.nasdaq.binaryfile;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUtils;
import net.openhft.chronicle.ChronicleConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by rmanaloto on 3/25/14.
 */
public class IndexedBinaryFile {

    private static final Logger log = LogManager.getLogger(IndexedBinaryFile.class);

    private long sequence = 0;

    private final String directoryPath;
    private final String baseFileName;
    private final String indexFileSuffix;
    private final String dataFileSuffix;
    private final Path directoryPathPath;
    private final Path dataFilePath;
    private final Path indexFilePath;
    private final File dataFile;
    private final File indexFile;

    private final ByteOrder byteOrder;

    private final int dataFileBlockSize;
    private final long dataFileInitialFileSize;
    private final long dataFileGrowBySize;

    private final int indexFileBlockSize;
    private final long indexFileInitialFileSize;
    private final long indexFileGrowBySize;

    private final boolean deleteIfExists;

    private final MappedFileBuffer dataMappedFile;
    private final MappedFileBuffer indexMappedFile;

    public IndexedBinaryFile(String directoryPath, String baseFileName,
                             int dataFileBlockSize, long dataFileInitialFileSize, long dataFileGrowBySize,
                             int indexFileBlockSize, long indexFileInitialFileSize, long indexFileGrowBySize,
                             boolean deleteIfExists,
                             ByteOrder byteOrder) throws IOException {
        this(directoryPath, baseFileName, "idx", "dat",
                dataFileBlockSize, dataFileInitialFileSize, dataFileGrowBySize,
                indexFileBlockSize, indexFileInitialFileSize, indexFileGrowBySize,
                deleteIfExists,
                byteOrder
                );
    }

    public IndexedBinaryFile(String directoryPath, String baseFileName, String indexFileSuffix, String dataFileSuffix,
                             int dataFileBlockSize, long dataFileInitialFileSize, long dataFileGrowBySize,
                             int indexFileBlockSize, long indexFileInitialFileSize, long indexFileGrowBySize,
                             boolean deleteIfExists,
                             ByteOrder byteOrder) throws IOException {
        this.directoryPath = directoryPath;
        this.baseFileName = baseFileName;
        this.indexFileSuffix = indexFileSuffix;
        this.dataFileSuffix = dataFileSuffix;

        this.dataFileBlockSize = dataFileBlockSize;
        this.dataFileInitialFileSize = dataFileInitialFileSize;
        this.dataFileGrowBySize = dataFileGrowBySize;

        this.indexFileBlockSize = indexFileBlockSize;
        this.indexFileInitialFileSize = indexFileInitialFileSize;
        this.indexFileGrowBySize = indexFileGrowBySize;

        this.byteOrder = byteOrder;
        this.deleteIfExists = deleteIfExists;

        this.directoryPathPath = Paths.get(directoryPath);
        this.dataFilePath = Paths.get(directoryPath, baseFileName + "." + dataFileSuffix);
        this.dataFile = this.dataFilePath.toFile();

        this.indexFilePath = Paths.get(directoryPath, baseFileName + "." + indexFileSuffix);
        this.indexFile = this.indexFilePath.toFile();

        dataMappedFile = new MappedFileBuffer(dataFile, this.dataFileBlockSize, this.dataFileInitialFileSize, this.dataFileGrowBySize, true, deleteIfExists);
//        dataMappedFile.setByteOrder(byteOrder);

        indexMappedFile = new MappedFileBuffer(indexFile, this.indexFileBlockSize, this.indexFileInitialFileSize, this.indexFileGrowBySize, true, deleteIfExists);
//        indexMappedFile.setByteOrder(byteOrder);

        initialize();
    }

    public IndexedBinaryFile(IndexedBinaryFileConfig config) throws IOException {
        this(config.getDirectoryPath(), config.getBaseFileName(), config.getIndexFileSuffix(), config.getDataFileSuffix(),
                config.getDataFileBlockSize(), config.getDataFileInitialFileSize(), config.getDataFileGrowBySize(),
                config.getIndexFileBlockSize(), config.getIndexFileInitialFileSize(), config.getIndexFileGrowBySize(),
                config.isDeleteIfExists(),
                config.getByteOrder())
        ;
    }

    private void initialize() {
        //setup 1st record for index 1
        indexMappedFile.putLong(1L, byteOrder);
        assert(indexMappedFile.position() == BitUtil.SIZE_OF_LONG);
        assert( indexMappedFile.getLong(0, byteOrder) == (1L));
        indexMappedFile.putLong(0L, byteOrder);
        assert(indexMappedFile.position() == (2*BitUtil.SIZE_OF_LONG));
        assert( indexMappedFile.getLong(8, byteOrder) == (0L));
    }

    public long getSequence() {
        return sequence;
    }

    public static final long INDEX_FILE_ROW_LENGTH = BitUtil.SIZE_OF_LONG*2; //8 bytes for sequence, and 8 bytes for payloadPosition

    public long increment(short messageLength, ByteBuffer payload) {
        sequence++;

        //indexFile
        final long indexedMappedFilePosition = indexMappedFile.position();
        indexMappedFile.putLong(sequence+1, byteOrder);
//        indexMappedFile.force();
        assert( indexMappedFile.getLong(indexedMappedFilePosition, byteOrder) == (sequence+1));
        final long dataMappedFilePosition = dataMappedFile.position();
        final int payloadLength = payload.remaining();
        long payloadPosition = dataMappedFilePosition + payloadLength + BitUtil.SIZE_OF_SHORT;
        indexMappedFile.putLong(payloadPosition, byteOrder);
//        indexMappedFile.force();
        assert( indexMappedFile.getLong(indexedMappedFilePosition + BitUtil.SIZE_OF_LONG, byteOrder) == payloadPosition);

        //dataFile
        dataMappedFile.putShort((short) payloadLength, byteOrder);
//        dataMappedFile.force();
        assert(dataMappedFile.position() == (dataMappedFilePosition+BitUtil.SIZE_OF_SHORT) );
        assert( dataMappedFile.getShort(dataMappedFilePosition, byteOrder) == payloadLength);
        dataMappedFile.putBytes(payload);
//        dataMappedFile.force();
        assert(dataMappedFile.position() == (dataMappedFilePosition+BitUtil.SIZE_OF_SHORT+payloadLength) );

//        indexMappedFile.force();
//        dataMappedFile.force();

        return sequence;
    }

    private long getPayloadPosition(long theSequence) {
        if(theSequence == 1) {
            return 0;
        }
        else if(theSequence > 1) {
            final long indexFilePosition = ((theSequence - 1) * INDEX_FILE_ROW_LENGTH) + BitUtil.SIZE_OF_LONG;
            final long payloadPosition = indexMappedFile.getLong(indexFilePosition, byteOrder);
            return payloadPosition;
        }
        else {
            throw new IllegalArgumentException("Sequence number should be greater than or equal to 1 But is " + theSequence);
        }
    }

    public long getMessageLength(long sequence) {
        assert(sequence > 0);
        final long payloadPosition = getPayloadPosition(sequence);
        final short messageLength = dataMappedFile.getShort(payloadPosition, byteOrder);
        return messageLength;
    }

    public byte getMessageType(long sequence) {
        assert(sequence > 0);
        final long payloadPosition = getPayloadPosition(sequence);
        final byte messageType = dataMappedFile.get(payloadPosition + BitUtil.SIZE_OF_SHORT);
        return messageType;
    }

    public short getMessage(long sequence, ByteBuffer destination) {
        assert(sequence > 0);
        final long payloadPosition = getPayloadPosition(sequence);
        final short messageLength = dataMappedFile.getShort(payloadPosition, byteOrder);
        assert(messageLength <= destination.remaining());
        dataMappedFile.getBytes(payloadPosition, destination, messageLength);
        return messageLength;
    }

    public void force() {
        indexMappedFile.force();
        dataMappedFile.force();
    }

    public static void main(String[] args) throws Exception {
        final BinaryFile binaryFile = getBinaryFile();
        final long binaryFileLength = binaryFile.length();

        FileSystem fileSystem = FileSystems.getDefault();
        Path directoryPath = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/");
        final String absoluteDirectoryPath = directoryPath.toFile().getAbsolutePath();
        String baseFileName = "testIndexedBinaryFile";

        String indexFileSuffix = "index";
        String dataFileSuffix = "data";
        int dataFileBlockSize = ChronicleConfig.SMALL.dataBlockSize();
        long dataFileInitialFileSize = binaryFileLength;
        long dataFileGrowBySize = ChronicleConfig.SMALL.dataBlockSize();

        int indexFileBlockSize = BitUtil.SIZE_OF_LONG*2*1_000_000; //accomodate 1,000,000 entries
        long indexFileInitialFileSize = BitUtil.SIZE_OF_LONG*2*50_000_000; //accomodate 50,000,000,000 entries
        long indexFileGrowBySize = BitUtil.SIZE_OF_LONG*2*1_000_000; //accomodate 1,000,000 entries
        boolean deleteIfExists = true;
        ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

        IndexedBinaryFile indexedBinaryFile = new IndexedBinaryFile(absoluteDirectoryPath, baseFileName, indexFileSuffix, dataFileSuffix,
                                                    dataFileBlockSize, dataFileInitialFileSize, dataFileGrowBySize,
                                                    indexFileBlockSize, indexFileInitialFileSize, indexFileGrowBySize,
                                                    deleteIfExists,
                                                    byteOrder);
        long currentSequence = indexedBinaryFile.getSequence();

        final ByteBuffer binaryFileByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE);
        final ByteBuffer indexedBinaryFileByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE);

        final long startTime = System.nanoTime();
        long binaryFileSequence = -1;
        while(binaryFile.hasNext()) {
            final short currentMessageLength = binaryFile.getCurrentMessageLength();

            binaryFileByteBuffer.clear();
            indexedBinaryFileByteBuffer.clear();

            binaryFile.next(binaryFileByteBuffer);
            binaryFileByteBuffer.flip();
            final long indexedBinaryFileSequence = indexedBinaryFile.increment(currentMessageLength, binaryFileByteBuffer);
            final long indexedBinaryFileMessageLength = indexedBinaryFile.getMessageLength(indexedBinaryFileSequence);
            assert(currentMessageLength == indexedBinaryFileMessageLength);
            binaryFileByteBuffer.flip();
            final byte messageType = binaryFileByteBuffer.get();
            final byte indexedBinaryFileMessageType = indexedBinaryFile.getMessageType(indexedBinaryFileSequence);
            assert(messageType == indexedBinaryFileMessageType);
            final ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
            if(itchMessageType == ITCHMessageType.NULL_VAL) {
                throw new RuntimeException("Null ITCHMessageType for " + binaryFile);
            }
            binaryFileSequence = binaryFile.getSequence();
            assert (indexedBinaryFileSequence == binaryFileSequence);
            if((binaryFileSequence < 10) || (binaryFileSequence % 1_000_000 == 0)) {
                log.info("[sequence=" + binaryFile.getSequence() + "][itchMessageType="+itchMessageType+"]");
            }

            indexedBinaryFile.getMessage(indexedBinaryFileSequence, indexedBinaryFileByteBuffer);
            indexedBinaryFileByteBuffer.equals(binaryFileByteBuffer);

            if( (indexedBinaryFileSequence % 1_000_000 == 0)) {
                indexedBinaryFile.force();
            }
        }
        final long endTime = System.nanoTime();
        long duration = endTime - startTime;
        log.info("end - [sequence=" + binaryFileSequence + "][duration="+ TimeUnit.NANOSECONDS.toMillis(duration)+"ms]");
    }

    private static BinaryFile getBinaryFile() throws IOException {
        FileSystem fileSystem = FileSystems.getDefault();
        Path path = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        long fileSize = file.length();
        String absolutePath = file.getAbsolutePath();
        int dataBlockSize = ChronicleConfig.SMALL.dataBlockSize();

        BinaryFile binaryFile = new BinaryFile(absolutePath, dataBlockSize, fileSize, dataBlockSize, ByteOrder.BIG_ENDIAN);
        return binaryFile;
    }
}
