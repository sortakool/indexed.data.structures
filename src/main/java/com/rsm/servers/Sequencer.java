package com.rsm.servers;

import com.rsm.servers.handlers.MoldUDP64Decoder;
import com.rsm.servers.handlers.TimestampSecondsCommandDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;

import java.net.InetSocketAddress;

/**
 * Server that receives Mold commands and then adds sequences to the messages and forwards them, can probably extend
 * the sequencer as it must have the same functionality
 */
public class Sequencer {

    private int port = 9999;

    private String commandMulticastcastGroup = "FF02:0:0:0:0:0:0:3";
    private String eventMulticastcastGroup = "FF02:0:0:0:0:0:0:4";
//    private String commandMulticastcastGroup = "230.0.0.1";

    //we need two
    private final ByteBuf eventByteBuf = Unpooled.directBuffer(1024);

    public void run() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(eventLoopGroup)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(new LoggingHandler());
//                            pipeline.addLast(new TimestampSecondsCommandDecoder(eventByteBuf));
                            pipeline.addLast(new MoldUDP64Decoder(eventByteBuf));
                        }
                    })
                    .localAddress(port)
                    ;
//                    .handler(new TimestampSecondsCommandDecoder(eventByteBuf));

            DatagramChannel channel = (DatagramChannel) bootstrap.bind().sync().channel();
            InetSocketAddress groupAddress = new InetSocketAddress(commandMulticastcastGroup, port);

            ChannelFuture channelFuture = channel.joinGroup(groupAddress, NetUtil.LOOPBACK_IF).sync();
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    System.out.println("done with " + future);
                }
            });

            channel.closeFuture().await();

        } finally {
            System.out.println("Sequencer shutting down");
            eventLoopGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Sequencer");

        Sequencer sequencer = new Sequencer();

        sequencer.run();
    }
}











