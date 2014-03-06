package com.rsm.servers.handlers;

import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * User: arhimmel
 * Date: 3/3/14
 * Time: 9:36 PM
 */
public class TimestampSecondsCommandDecoder extends MessageToMessageDecoder<TimestampSecondsCommand> {
    Logger logger = Logger.getLogger(this.getClass());

    final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();

    private final ByteBuf byteBuf;
    private final ByteBuffer byteBuffer;
    final DirectBuffer commandDirectBuffer;

    public TimestampSecondsCommandDecoder(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
        this.byteBuffer = byteBuf.nioBuffer();
        this.commandDirectBuffer = new DirectBuffer(byteBuffer);

    }


    @Override
    protected void decode(ChannelHandlerContext ctx, TimestampSecondsCommand msg, List<Object> out) throws Exception {
        logger.info("got a time command");

        byteBuf.clear();

        timestampSecondsCommand.wrapForDecode(commandDirectBuffer, byteBuffer.position(),
                TimestampSecondsCommand.BLOCK_LENGTH, TimestampSecondsCommand.SCHEMA_VERSION);


    }
}
