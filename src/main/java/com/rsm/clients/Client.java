package com.rsm.clients;

import com.rsm.clients.handlers.DatagramWrapperHandler;
import com.rsm.clients.handlers.MoldCommandOutgoingHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;

import java.net.InetSocketAddress;


/**
 * Basic client that will send a basic message sleep and then send the message again
 */
public class Client {
    private final Bootstrap bootstrap;
    private final EventLoopGroup group;
    private InetSocketAddress groupAddress;

    private int port = 9999;
    private int counter = 0;
    private String mCastGroup = "FF02:0:0:0:0:0:0:3";
    private String name = "test";

    public Client() {
        group = new NioEventLoopGroup();
        groupAddress = new InetSocketAddress(mCastGroup, port);

        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
                .handler(new ChannelInitializer<DatagramChannel>() {

                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(new LoggingHandler())
                                .addLast(new MoldCommandOutgoingHandler())
                                .addLast(new DatagramWrapperHandler(groupAddress));
                    }
                }).localAddress(port);
    }

    public void run() throws InterruptedException {
        DatagramChannel ch = (DatagramChannel) bootstrap.bind().sync().channel();


        ch.joinGroup(groupAddress, NetUtil.LOOPBACK_IF).sync();

        for (;;) {
            System.out.println("Sending");
//            ch.write("Hello");
//            ch.writeAndFlush(
//                    new DatagramPacket(Unpooled.copyInt(counter++), groupAddress)
//            );

            ch.write(counter++);
            ch.flush();
            Thread.sleep(1000);
        }
    }

    private void stop() {
        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Starting client");
        Client client = new Client();

        try {
            client.run();
        } finally {
            client.stop();
        }
    }

}
