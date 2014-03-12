package com.rsm.servers.handlers;

import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsEvent;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * User: arhimmel
 * Date: 3/3/14
 * Time: 9:36 PM
 */
public class TimestampSecondsCommandDecoder extends MessageToMessageDecoder<DatagramPacket> {
    Logger logger = Logger.getLogger(this.getClass());

    private final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
    private final TimestampSecondsEvent timestampSecondsEvent = new TimestampSecondsEvent();

    private final ByteBuf eventByteBuf;
    private final ByteBuffer eventByteBuffer;
    final DirectBuffer eventDirectBuffer;

    private long sequence = 0;

    public TimestampSecondsCommandDecoder(ByteBuf eventByteBuf) {
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

        timestampSecondsCommand.wrapForDecode(commandDirectBuffer, commandByteBuffer.position(),
                TimestampSecondsCommand.BLOCK_LENGTH, TimestampSecondsCommand.SCHEMA_VERSION);

        long timestampNanos = timestampSecondsCommand.streamHeader().timestampNanos();
        byte major = timestampSecondsCommand.streamHeader().major();
        byte minor = timestampSecondsCommand.streamHeader().minor();
        long source = timestampSecondsCommand.streamHeader().source();
        long id = timestampSecondsCommand.streamHeader().id();
        long ref = timestampSecondsCommand.streamHeader().ref();
        ITCHMessageType itchMessageType = timestampSecondsCommand.messageType();
        long seconds = timestampSecondsCommand.seconds();

        logger.info("[seq="+sequence+"][major="+major+"][minor="+minor+"][id="+id+"][ref="+ref+"]");

        //send out event (from command)
        eventByteBuf.clear();
    }
}