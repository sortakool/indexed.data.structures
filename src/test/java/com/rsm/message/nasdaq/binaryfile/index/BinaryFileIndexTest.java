package com.rsm.message.nasdaq.binaryfile.index;

import com.rsm.buffer.MappedFileBuffer;
import org.junit.Assert;
import org.junit.Test;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by rmanaloto on 3/1/14.
 */
public class BinaryFileIndexTest {


    private final Random random = new Random();

    @Test
    public void testSequencerIndex() throws Exception {
        Path path = Paths.get("/Users/rmanaloto/Downloads/binaryFileIndex.idx");
        File file = path.toFile();
        file.setWritable(true);
        file.deleteOnExit();
        boolean newFile = file.createNewFile();
        MappedFileBuffer fileBuffer = new MappedFileBuffer(file);
//        final ByteBuffer buffer = ByteBuffer.allocateDirect(4096);


        final short messageTemplateVersion = 0;
        int bufferOffset = 0;
        int encodingLength = 0;

        MappedByteBuffer byteBuffer = fileBuffer.buffer(bufferOffset);
//        MappedByteBuffer byteBuffer = fileBuffer.buffer(buffer);
        final DirectBuffer directBuffer = new DirectBuffer(byteBuffer);
//        final DirectBuffer directBuffer = new DirectBuffer(buffer);

        // Setup for encoding a message
        BinaryFileIndex binaryFileIndex = new BinaryFileIndex();
//        MessageHeader messageHeader = new MessageHeader();
//        messageHeader.wrap(directBuffer, bufferOffset, messageTemplateVersion)
//                .blockLength(binaryFileIndex.sbeBlockLength())
//                .templateId(binaryFileIndex.sbeTemplateId())
//                .schemaId(binaryFileIndex.sbeSchemaId())
//                .version(binaryFileIndex.sbeSchemaVersion());
//
//        bufferOffset += messageHeader.size();
//        encodingLength += messageHeader.size();

        final long startSequence = 1;
        final long maxSequence = 1000L;

        Map<Long, Long> map = new HashMap<>((int)maxSequence, 1.0f);

        long sequence;
        sequence = startSequence;
        long filePosiiton = 0;

        //encode
        final int srcOffset = 0;
        binaryFileIndex.wrapForEncode(directBuffer, bufferOffset);
        SequencePositionMap sequencePositionMap = binaryFileIndex.sequencePositions();
        for(; sequence<=maxSequence; sequence++) {
            sequencePositionMap.wrap(directBuffer, bufferOffset, 0);
            sequencePositionMap.sequence(sequence);
            sequencePositionMap.position(filePosiiton);
            map.put(sequence, filePosiiton);
            filePosiiton += random.nextInt(100);
            encodingLength += sequencePositionMap.size();
            bufferOffset = encodingLength;
        }

        //decode
        bufferOffset = 0;
//        messageHeader.wrap(directBuffer, bufferOffset, messageTemplateVersion);
//        // Lookup the applicable flyweight to decode this type of message based on templateId and version.
//        final int templateId = messageHeader.templateId();
//        if (templateId != binaryFileIndex.TEMPLATE_ID)
//        {
//            throw new IllegalStateException("Template ids do not match");
//        }
//        final int actingBlockLength = messageHeader.blockLength();
//        final int schemaId = messageHeader.schemaId();
//        final int actingVersion = messageHeader.version();
//
//        bufferOffset += messageHeader.size();

        sequence = startSequence;
//        binaryFileIndex.wrapForDecode(directBuffer, bufferOffset, actingBlockLength, actingVersion);
        sequencePositionMap = binaryFileIndex.sequencePositions();
        for(; sequence<=maxSequence; sequence++) {
            sequencePositionMap.wrap(directBuffer, bufferOffset, 0);
            long decodedSequence = sequencePositionMap.sequence();
            long decodedPosition = sequencePositionMap.position();
            Assert.assertEquals(sequence, decodedSequence);
            Assert.assertEquals(map.get(sequence).longValue(), decodedPosition);
            bufferOffset += sequencePositionMap.size();
        }
    }
}
