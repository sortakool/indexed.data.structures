package com.rsm.servers;

import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;

/**
 * Server that receives Mold Event and then adds sequences to the messages and forwards them
 */
public class Rewind {

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    public static final String TCP_REWIND_IP = "FF02:0:0:0:0:0:0:5";
    public static final int TCP_REWIND_PORT = 9002;

    private ServerSocketChannel tcpSocketChannel;
    private DatagramChannel eventChannel;

    public Rewind() throws Exception {
        Selector selector = Selector.open();

        //create multicast connection
        InetSocketAddress eventGroup = new InetSocketAddress(EVENT_MULTICAST_IP, EVENT_MULTICAST_PORT);
        eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        eventChannel.bind(null);
//        eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
//        eventChannel.setOption(StandardSocketOptions.SO_SNDBUF, eventByteBuffer.capacity()*2);
        eventChannel.configureBlocking(false);
        final SelectionKey readableSelectionKey = eventChannel.register(selector, SelectionKey.OP_READ);

        //create TCP connection
        tcpSocketChannel = ServerSocketChannel.open();
        tcpSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(TCP_REWIND_IP, TCP_REWIND_PORT);
        tcpSocketChannel.bind(inetSocketAddress);
        tcpSocketChannel.configureBlocking(false);

        //we only care right now that we are ready to connect
        final SelectionKey tcpRequest = tcpSocketChannel.register(selector, SelectionKey.OP_CONNECT);

        boolean active = true;
        while(active) {
            int updated = selector.selectNow();
            if (updated > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey selectionKey = iter.next();
                    iter.remove();

                    if (tcpRequest.isConnectable()) {
                        //we have been connected to and need to read in the request from
                        //the client requesting a rewind
                    } else if (readableSelectionKey.isReadable()) {
                        //we got a message and should save it to our binary file
                    }
                }
            }
        }

    }


    public static void main(String[] args) throws Exception{
        Rewind rewind = new Rewind();
    }

}
