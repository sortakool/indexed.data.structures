package com.rsm.clients;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Enumeration;

/**
 * Created by rmanaloto on 3/18/14.
 */
public class Client4 {

    private static final Logger log = LogManager.getLogger(Client4.class);

    public static void main(String[] args) throws Exception {
        String MULTICAST_IP = "239.1.1.1";
        int MULTICAST_PORT = 8989;

        DatagramChannel server = null;
        try {
            server = DatagramChannel.open();
            server.bind(null);

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

            int mtu = networkInterface.getMTU();
            log.info("mtu=" + mtu);
            server.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

            String msg = "hello";
//            ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
            ByteBuffer buffer = ByteBuffer.allocateDirect(msg.length());
            buffer.put(msg.getBytes(), 0, msg.length());
            buffer.flip();
            InetSocketAddress group = new InetSocketAddress(MULTICAST_IP, MULTICAST_PORT);

            int bytesSent = server.send(buffer, group);
            log.info("bytesSent=" + bytesSent);
        }
        finally {
            server.close();
        }



    }
}
