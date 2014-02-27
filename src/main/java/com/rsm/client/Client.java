package com.rsm.client;

import com.rsm.client.handlers.MoldCommandOutgoingHandler;
import com.rsm.servers.handlers.MoldCommandIncomingHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.DatagramPacket;

/**
 * Basic client that will send a basic message sleep and then send the message again
 */
public class Client {
    private int port = 9999;
    private int counter = 0;
    private String name = "test";


    public void run() throws InterruptedException {
        EventLoopGroup g = new NioEventLoopGroup();

        final MoldCommandOutgoingHandler handler = new MoldCommandOutgoingHandler();

        try {
            Bootstrap b = new Bootstrap();
            b.group(g)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    .handler(new ChannelInitializer<DatagramChannel>() {

                        @Override
                        protected void initChannel(DatagramChannel ch) throws Exception {
                            ch.pipeline().addLast(handler);
                        }
                    });

            ChannelFuture future = b.connect("localhost", port).sync();

            future.channel().writeAndFlush("This is a test");

            future.channel().closeFuture().sync();

        } finally {
            System.out.println("Client shutting down");
            g.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Starting client");
        Client client = new Client();

        client.run();
    }

}
