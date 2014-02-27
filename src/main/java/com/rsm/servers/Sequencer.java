package com.rsm.servers;

import com.rsm.servers.handlers.MoldCommandIncomingHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * Server that receives Mold commands and then adds sequences to the messages and forwards them, can probably extend
 * the sequencer as it must have the same functionality
 */
public class Sequencer {

    private int port = 9999;


    //we need two

    public void run() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(eventLoopGroup)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .handler(new MoldCommandIncomingHandler());

            bootstrap.bind(port).sync().channel().closeFuture().await();
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











