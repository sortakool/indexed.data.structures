package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

/**
 * see http://books.google.com/books?id=kELcexu0pAcC&pg=PA371&lpg=PA371&dq=java+multicast+bytebuffer&source=bl&ots=SyEWLrM71V&sig=kLUEnaQxv3zBIO7VuQv4cHAsrE8&hl=en&sa=X&ei=TeQjU6DsO6H7yAHv2IGgDQ&ved=0CGsQ6AEwBg#v=onepage&q=java%20multicast%20bytebuffer&f=false
 * Created by rmanaloto on 3/18/14.
 */
public class Client5 {

    private static final Logger log = LogManager.getLogger(Client5.class);

    //        String MULTICAST_IP = "239.1.1.1";
    String MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    int MULTICAST_PORT = 9999;

    private final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    private final byte[] sessionBytes = "0123456789".getBytes();

    Path path;
    File file;
    MappedFileBuffer fileBuffer;
    MappedByteBuffer fileByteBuffer;
    DirectBuffer fileDirectBuffer;
    long filePosition = 0;

    ByteBuffer commandByteBuffer;
    DirectBuffer commandDirectBuffer;
    long commandPosition = 0;

    private int sequence = 0;

    public Client5() throws Exception {
        path = Paths.get(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
        fileBuffer = new MappedFileBuffer(file);
        filePosition = fileBuffer.position();
        long capacity = fileBuffer.capacity();
        fileByteBuffer = fileBuffer.buffer(filePosition);
        fileDirectBuffer = new DirectBuffer(fileByteBuffer);

        commandByteBuffer = ByteBuffer.allocateDirect(2048);
//        commandByteBuffer = ByteBuffer.allocate(2048);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        InetSocketAddress group = new InetSocketAddress(MULTICAST_IP, MULTICAST_PORT);


        DatagramChannel server = null;
        try {
            server = DatagramChannel.open(StandardProtocolFamily.INET6);
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
            server.setOption(StandardSocketOptions.SO_SNDBUF, 1024*128);
            server.configureBlocking(false);

            Selector selector = Selector.open();
            server.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

            String msg = "hello";
//            ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
//            ByteBuffer commandByteBuffer = ByteBuffer.allocateDirect(msg.length());

            boolean active = true;
            while(active) {
                int selected = selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (key.isAcceptable()) {

                    }
                    else if (key.isConnectable()) {

                    }
                    else if (key.isReadable()) {

                    }
                    else if (key.isWritable()) {
                        short messageLength = fileDirectBuffer.getShort((int) filePosition, ByteOrder.BIG_ENDIAN);
                        if(messageLength == 0) {
                            active = false;
                        }
                        long nextPosition = filePosition + 2 + messageLength;
                        fileByteBuffer.position((int) filePosition);
                        if(nextPosition > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException("Invalid position " + nextPosition);
                        }
                        fileByteBuffer.limit((int) nextPosition);

                        //create MoldUDP64 Packet
                        long startingCommandPosition = commandPosition;
                        moldUDP64Packet.wrapForEncode(commandDirectBuffer, (int)commandPosition);
                        // Downstream Packet Message Block
                        moldUDP64Packet.downstreamPacketHeader()
                                .putSession(sessionBytes, (int) commandPosition)
                                .sequenceNumber(++sequence)
                                .messageCount(1);//hard code to 1 for now
                        int moldUDP64PacketLength = moldUDP64Packet.size();
                        commandByteBuffer.position((int)startingCommandPosition);
                        //messageLength
                        commandDirectBuffer.putShort((int)commandPosition, messageLength, ByteOrder.BIG_ENDIAN);
                        int bytesRead = commandDirectBuffer.getBytes((int) commandPosition, fileByteBuffer, messageLength);
                        assert (bytesRead == messageLength);
                        commandByteBuffer.position((int)startingCommandPosition);
                        int nextCommandLimit = moldUDP64PacketLength + 2 + messageLength;
                        commandByteBuffer.limit(nextCommandLimit);

                        int bytesSent = server.send(commandByteBuffer, group);
                        if(sequence % 1000000 == 0) {
                            log.info("[seq=" + sequence + "][filePosition="+filePosition+"][messageLength=" + messageLength + "][bytesSent=" + bytesSent + "]");
                        }

                        filePosition = nextPosition;    //2 is for messageLength
                    }
                }
            }
            log.info("finished with seq="+sequence);
        }
        finally {
            server.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Client5 client = new Client5();
    }
}
