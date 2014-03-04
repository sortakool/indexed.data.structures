package com.rsm.servers.handlers;

import com.rsm.message.nasdaq.itch.v4_1.TimestampSecondsCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.util.List;

/**
 * User: arhimmel
 * Date: 3/3/14
 * Time: 9:36 PM
 */
public class TimestampSecondsCommandDecoder extends MessageToMessageDecoder<TimestampSecondsCommand> {
    @Override
    protected void decode(ChannelHandlerContext ctx, TimestampSecondsCommand msg, List<Object> out) throws Exception {
        msg.wrapForDecode(new DirectBuffer(ctx.alloc().buffer().array()), 0, 0, 0);
        out.add(msg);
    }
}
