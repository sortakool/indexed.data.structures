package com.rsm.clients.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * User: arhimmel
 * Date: 2/26/14
 * Time: 6:47 PM
 */
public class MoldCommandOutgoingHandler extends MessageToMessageEncoder<Integer> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Integer msg, List<Object> out) throws Exception {
        System.out.println("Mold command out");
        out.add(0, 1);
        out.add(1, msg);

    }


//    @Override
//    protected void encode(ChannelHandlerContext ctx, CharSequence msg, ByteBuf out) throws Exception {
//        out.writeInt(msg.length());
//        ByteBufUtil.encodeString(out.alloc(), CharBuffer.wrap(msg), Charset.defaultCharset());
//    }
}