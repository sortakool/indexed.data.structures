package com.rsm.servers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.*;
import java.nio.channels.*;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Server that receives Mold Event and then adds sequences to the messages and forwards them
 */
public class Rewind {

    private static final Logger log = LogManager.getLogger(Rewind.class);

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    public static final String TCP_REWIND_IP = "FF02:0:0:0:0:0:0:5";
    public static final int TCP_REWIND_PORT = 9002;

    private ServerSocketChannel tcpSocketChannel;
    private DatagramChannel eventChannel;

    MembershipKey eventMembershipKey;

    public Rewind() throws Exception {
        Selector selector = Selector.open();

        NetworkInterface networkInterface = getNetworkInterface();

        //connect to the event group
        eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        eventChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress eventInetSocketAddress = new InetSocketAddress(EVENT_MULTICAST_PORT);
        eventChannel.bind(eventInetSocketAddress);
        eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        InetAddress eventGroup = InetAddress.getByName(EVENT_MULTICAST_IP);
        eventMembershipKey = eventChannel.join(eventGroup, networkInterface);

        eventChannel.configureBlocking(false);
        final SelectionKey readableSelectionKey = eventChannel.register(selector, SelectionKey.OP_READ);


        //create TCP connection
        tcpSocketChannel = ServerSocketChannel.open();
        tcpSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(TCP_REWIND_IP, TCP_REWIND_PORT);
        tcpSocketChannel.bind(null);
        tcpSocketChannel.configureBlocking(false);

        //we only care right now that we are ready to connect
        final SelectionKey tcpRequestSelectionKey = tcpSocketChannel.register(selector, 0);

        boolean active = true;
        while(active) {
            int updated = selector.selectNow();
            if (updated > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey selectionKey = iter.next();
                    iter.remove();

                    if (tcpRequestSelectionKey.isConnectable()) {
                        //we have been connected to and need to read in the request from
                        //the client requesting a rewind
                    } else if (readableSelectionKey.isReadable()) {
                        //we got a message and should save it to our binary file
                        log.info("got something");


                    }
                }
            }
        }

    }

    private NetworkInterface getNetworkInterface() throws SocketException {
        // Get the reference of a network interface
        NetworkInterface networkInterface = null;
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while(networkInterfaces.hasMoreElements()) {
            NetworkInterface nextNetworkInterface = networkInterfaces.nextElement();
            log.info(nextNetworkInterface);
            if(nextNetworkInterface.supportsMulticast()) {
                networkInterface = nextNetworkInterface;
//                break;
            }
        }
        return networkInterface;
    }

    public static void main(String[] args) throws Exception{
        Rewind rewind = new Rewind();
    }

}
