package com.rsm.servers.handlers;

import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsEvent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * User: arhimmel
 * Date: 3/3/14
 * Time: 9:36 PM
 */
public class MoldUDP64Decoder extends MessageToMessageDecoder<DatagramPacket> {
    Logger logger = Logger.getLogger(this.getClass());

    private final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    private final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
    private final TimestampSecondsEvent timestampSecondsEvent = new TimestampSecondsEvent();

    private final byte[] sessionBytes = new byte[10];
    private final byte[] payloadBytes = new byte[1024];


    private final ByteBuf eventByteBuf;
    private final ByteBuffer eventByteBuffer;
    final DirectBuffer eventDirectBuffer;

    private long sequence = 0;

    public MoldUDP64Decoder(ByteBuf eventByteBuf) {
        this.eventByteBuf = eventByteBuf;
        this.eventByteBuffer = eventByteBuf.nioBuffer(0, this.eventByteBuf.capacity());
        this.eventDirectBuffer = new DirectBuffer(eventByteBuffer);

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket datagramPacket, List<Object> out) throws Exception {
        sequence++;
        ByteBuf commandByteBuf = datagramPacket.content();
        ByteBuffer commandByteBuffer = commandByteBuf.nioBuffer();
        DirectBuffer commandDirectBuffer = new DirectBuffer(commandByteBuffer);

        int position = commandByteBuffer.position();
        moldUDP64Packet.wrapForDecode(commandDirectBuffer, position, MoldUDP64Packet.BLOCK_LENGTH, MoldUDP64Packet.SCHEMA_VERSION);
        Arrays.fill(sessionBytes, (byte)' ');
        moldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
        long sequenceNumber = moldUDP64Packet.downstreamPacketHeader().sourceSequence();
        int messageCount = moldUDP64Packet.downstreamPacketHeader().messageCount();

        position += moldUDP64Packet.size();

        short messageLength = commandDirectBuffer.getShort(position, ByteOrder.BIG_ENDIAN);
        position += 2;
        commandDirectBuffer.getBytes(position, payloadBytes, 0, messageLength);
        position += messageLength;

        logger.info("[session="+new String(sessionBytes)+"][seq="+sequenceNumber+"]");

        //send out event (from command)
        eventByteBuf.clear();
    }
}