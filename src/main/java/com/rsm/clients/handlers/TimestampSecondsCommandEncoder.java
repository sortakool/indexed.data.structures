package com.rsm.clients.handlers;

import com.rsm.message.nasdaq.itch.v4_1.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ReferenceCountUtil;
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
public class TimestampSecondsCommandEncoder extends MessageToMessageEncoder<TimestampSecondsMessage> {
    Logger logger = Logger.getLogger(this.getClass());

    final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();

    private int counter = 0;

    private final DatagramPacket datagramPacket;
    private final ByteBuf byteBuf;
    private final ByteBuffer byteBuffer;
    private final DirectBuffer commandDirectBuffer;
    private final InetSocketAddress remoteAddress;

    private final long source = 1L; //convert a 8-bit ascii to a long

    private final byte[] sessionBytes = "0123456789".getBytes();


    public TimestampSecondsCommandEncoder(ByteBuf byteBuf, InetSocketAddress remoteAddress) {
        this.byteBuf = byteBuf;
        this.byteBuffer = byteBuf.nioBuffer(0, this.byteBuf.capacity());
        this.commandDirectBuffer = new DirectBuffer(byteBuffer);
        this.remoteAddress = remoteAddress;
        datagramPacket = new DatagramPacket(this.byteBuf, remoteAddress);

        this.counter = 0;//should be source specific id sequence
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, TimestampSecondsMessage msg, List<Object> out) throws Exception {
//        logger.info("Sending some stuff");

        counter++;

        byteBuf.clear();
        int position;
        position = byteBuf.writerIndex();

        //create MoldUDP64 Packet
        moldUDP64Packet.wrapForEncode(commandDirectBuffer, position);
        // Downstream Packet Message Block
        moldUDP64Packet.downstreamPacketHeader()
                .putSession(sessionBytes, position)
                .sequenceNumber(counter)
                .messageCount(1);//hard code to 1 for now

        position += moldUDP64Packet.size();

        //messageLength
        commandDirectBuffer.putShort(position, (short)timestampSecondsCommand.sbeBlockLength(), java.nio.ByteOrder.BIG_ENDIAN);
        position += 2;

        //create command
        timestampSecondsCommand.wrapForEncode(commandDirectBuffer, position);
        //Downstream Packet Message Block
        //MessageData
        // StreamHeader
        StreamHeader streamHeader = timestampSecondsCommand.streamHeader();
        streamHeader
                .timestampNanos(System.nanoTime())
                .major((byte)'S')
                .minor(msg.messageType().value())
                .source(source)
                .id(counter)
                .ref(counter)
        ;
        timestampSecondsCommand.messageType(msg.messageType());
        long seconds = msg.seconds();
        timestampSecondsCommand.seconds(seconds);

        int size = timestampSecondsCommand.size();
        position += size;
        byteBuffer.position(position);
        byteBuffer.flip();

        byteBuf.writeBytes(byteBuffer);

        out.add(datagramPacket);
        ReferenceCountUtil.retain(this.byteBuf);
        ReferenceCountUtil.retain(msg);
        ReferenceCountUtil.retain(datagramPacket);
//        ctx.channel().flush();
        ctx.channel().flush();

        logger.info("[counter="+counter+"][seconds="+seconds+"]");

    }
}

