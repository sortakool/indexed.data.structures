package com.rsm.buffer;

import com.rsm.message.nasdaq.binaryfile.BinaryFile;
import com.rsm.message.nasdaq.binaryfile.index.BinaryFileIndex;
import com.rsm.message.nasdaq.binaryfile.index.SequencePositionMap;
import com.rsm.message.nasdaq.itch.v4_1.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by rmanaloto on 2/27/14.
 */
public class ItchParser2 {

    private static final Logger log = LogManager.getLogger(MappedFileBuffer.class);

    final TimestampSecondsMessage timestampSecondsMessage = new TimestampSecondsMessage();
    final SystemEventMessage systemEventMessage = new SystemEventMessage();
    final StockDirectoryMessage stockDirectoryMessage = new StockDirectoryMessage();

    public ItchParser2() throws Exception {
        Path path = Paths.get("/Users/rmanaloto/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        MappedFileBuffer fileBuffer = new MappedFileBuffer(file);

//        BinaryFile binaryFile = new BinaryFile();

        long position = fileBuffer.position();

        MappedByteBuffer byteBuffer = fileBuffer.buffer(position);
        final DirectBuffer directBuffer = new DirectBuffer(byteBuffer);

        //public BinaryFile wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
//        binaryFile.wrapForDecode(directBuffer, (int)position, binaryFile.sbeBlockLength(), binaryFile.sbeSchemaVersion());
//
//        final byte[] messageBuffer = new byte[1024];
//        DirectBuffer messageDirectBuffer = new DirectBuffer(messageBuffer);

        final byte[] commandBuffer = new byte[1024];
        DirectBuffer commandDirectBuffer = new DirectBuffer(commandBuffer);

        final byte[] eventBuffer = new byte[1024];
        DirectBuffer eventDirectBuffer = new DirectBuffer(eventBuffer);

        byte[] temp = new byte[1024];

        long seq = 0;
        while(true) {
            log.info("position="+position);

            int messageLength = directBuffer.getShort((int) position, ByteOrder.BIG_ENDIAN);
            if(messageLength == 0) {
                break;
            }
            seq++;

            position += 2;  //move position past length

            byte messageType = directBuffer.getByte((int)position);
            ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);

            ITCHMessageType retrievedItchMessageType;



            switch(itchMessageType) {
                case TIMESTAMP_SECONDS:
                    timestampSecondsMessage.wrapForDecode(directBuffer, (int)position, timestampSecondsMessage.sbeBlockLength(), timestampSecondsMessage.sbeSchemaVersion());
                    retrievedItchMessageType = timestampSecondsMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long seconds = timestampSecondsMessage.seconds();
                    log.info("[seconds="+seconds+"]");

                    //create command
                    TimestampSecondsType timestampSecondsType = new TimestampSecondsType();
                    timestampSecondsType.wrap(directBuffer, (int)position, timestampSecondsMessage.sbeSchemaVersion());
                    long seconds1 = timestampSecondsType.seconds();
                    log.info("[seconds="+seconds1+"]");
                    assert (seconds == seconds1);

                    TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
                    timestampSecondsCommand.wrapForEncode(commandDirectBuffer, 0);
                    StreamHeader streamHeader = timestampSecondsCommand.streamHeader();
                    streamHeader
                            .timestampNanos(System.nanoTime())
                            .major((byte)'S')
                            .minor(messageType)
                            .source(1L)//convert a 8-bit ascii to a long
                            .id(seq)//should be source specific id sequence
                            .ref(seq)
                    ;

                    break;
                case SYSTEM_EVENT:
                    systemEventMessage.wrapForDecode(directBuffer, (int)position, systemEventMessage.sbeBlockLength(), systemEventMessage.sbeSchemaVersion());
                    retrievedItchMessageType = systemEventMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long timestamp = systemEventMessage.timestamp();
                    byte eventCode = systemEventMessage.eventCode();
                    log.info("[timestamp="+timestamp+"][eventCode="+eventCode+"]");
                    break;
                case STOCK_DIRECTORY:
                    stockDirectoryMessage.wrapForDecode(directBuffer, (int)position, stockDirectoryMessage.sbeBlockLength(), stockDirectoryMessage.sbeSchemaVersion());
                    retrievedItchMessageType = stockDirectoryMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long nanoseconds = stockDirectoryMessage.nanoseconds();
                    stockDirectoryMessage.getStock(temp, 0);
                    log.info("[nanoseconds="+nanoseconds+"][stock="+new String(temp, 0, StockDirectoryMessage.stockLength())+"]");
                    break;
                default:
                    log.error("Unhandled type: " + (char)messageType);
                    break;
            }

            position += messageLength;

        }
        log.info("finished");
    }


    public static void main(String[] args) throws Exception {
        ItchParser2 paser = new ItchParser2();
    }
}
