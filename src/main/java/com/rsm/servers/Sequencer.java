package com.rsm.servers;

import com.rsm.servers.handlers.MoldCommandIncomingHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;
import io.netty.channel.socket.DatagramChannel;

import java.net.InetSocketAddress;

/**
 * Server that receives Mold commands and then adds sequences to the messages and forwards them, can probably extend
 * the sequencer as it must have the same functionality
 */
public class Sequencer {

    private int port = 9999;

    private String mCastGroup = "FF02:0:0:0:0:0:0:3";

    //we need two

    public void run() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(eventLoopGroup)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
                    .handler(new LoggingHandler())
                    .handler(new MoldCommandIncomingHandler());

            DatagramChannel channel = (DatagramChannel) bootstrap.bind(port).sync().channel();
            InetSocketAddress groupAddress = new InetSocketAddress(mCastGroup, port);

            channel.joinGroup(groupAddress, NetUtil.LOOPBACK_IF).sync();

            channel.closeFuture().syncUninterruptibly();

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











