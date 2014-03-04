package com.rsm.clients.handlers;

import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.InetSocketAddress;
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

        msg.streamHeader().timestampNanos(6).id(1);
        msg.payload().seconds(6);
        msg.wrapForEncode(new DirectBuffer(ctx.alloc().buffer().nioBuffer()), 0);


        out.add(msg);

    }
}
