package com.rsm.clients.handlers;

import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import com.rsm.message.nasdaq.itch.v4_1.StreamHeader;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
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
public class BinaryFileCommandEncoder extends MessageToMessageEncoder<ByteBuffer> {
    Logger logger = Logger.getLogger(this.getClass());

    final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();

    private int counter = 0;

    private final DatagramPacket datagramPacket;
    private final ByteBuf byteBuf;
    private final ByteBuffer byteBuffer;
    private final DirectBuffer commandDirectBuffer;
    private final InetSocketAddress remoteAddress;

    private final long source = 1L; //convert a 8-bit ascii to a long

    private final byte[] sessionBytes = "0123456789".getBytes();

    public BinaryFileCommandEncoder(ByteBuf byteBuf,InetSocketAddress remoteAddress) {
        this.byteBuf = byteBuf;
        this.byteBuffer = byteBuf.nioBuffer(0, this.byteBuf.capacity());
        this.commandDirectBuffer = new DirectBuffer(byteBuffer);
        this.remoteAddress = remoteAddress;
        datagramPacket = new DatagramPacket(this.byteBuf, this.remoteAddress);

        this.counter = 0;//should be source specific id sequence
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuffer theByteBuffer, List<Object> out) throws Exception {
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

        int remaining = theByteBuffer.remaining();

        byteBuf.getBytes(position, theByteBuffer);

        position += remaining;
        byteBuffer.position(position);
        byteBuffer.flip();

        out.add(datagramPacket);
        ReferenceCountUtil.retain(this.byteBuf);
        ReferenceCountUtil.retain(datagramPacket);
//        ctx.channel().flush();
        ctx.channel().flush();

        logger.info("[counter="+counter+"][remaining="+remaining+"]");

    }
}

