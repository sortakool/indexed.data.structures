package com.rsm.servers.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Server side handler for mold commands
 */
public class MoldCommandIncomingHandler extends SimpleChannelInboundHandler<DatagramPacket> {


//    @Override
//    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
//        System.out.println("Got it");
//        int length = in.readInt();
//        System.out.println(in.readBytes(length));
//    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
        System.out.println("Mold incoming");
        System.out.println(msg.content().getInt(0));

    }
//
//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        ctx.flush();
//    }


}
