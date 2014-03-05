package com.rsm.clients.handlers;

import com.rsm.message.nasdaq.itch.v4_1.MessageHeader;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * User: arhimmel
 * Date: 3/3/14
 * Time: 8:27 PM
 */
public class TimestampSecondsCommandEncoder extends MessageToMessageEncoder<TimestampSecondsCommand> {
    Logger logger = Logger.getLogger(this.getClass());

    @Override
    protected void encode(ChannelHandlerContext ctx, TimestampSecondsCommand msg, List<Object> out) throws Exception {
        logger.info("Sending some stuff");

        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);

        final DirectBuffer buf = new DirectBuffer(byteBuffer);

        int bufferOffset = 0;
        int encodingLength = 0;

//        msg.streamHeader().timestampNanos(6).id(1);
//        msg.payload().seconds(6);
//        msg.wrapForEncode(buf, 0);
        MessageHeader messageHeader = new MessageHeader();

        messageHeader.wrap(buf, bufferOffset, 0)
                .blockLength(msg.sbeBlockLength())
                .templateId(msg.sbeTemplateId())
                .schemaId(msg.sbeSchemaId())
                .version(msg.sbeSchemaVersion());

        bufferOffset += messageHeader.size();
        encodingLength += messageHeader.size();
        encodingLength += encode(msg, buf, bufferOffset);


        out.add(msg);

    }

    private int encode(final TimestampSecondsCommand msg, final DirectBuffer directBuffer, final int bufferOfset) {
        final int srcOffset = 0;

        msg.wrapForEncode(directBuffer, bufferOfset).payload().seconds(100);

        return msg.size();

    }
}