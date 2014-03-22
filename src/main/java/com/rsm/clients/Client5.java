package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.message.nasdaq.itch.v4_1.ITCHMessageType;
import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import com.rsm.message.nasdaq.itch.v4_1.StreamHeader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.net.*;
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
    private final StreamHeader streamHeader = new StreamHeader();
    private final int streamHeaderVersion = 1;
    private final byte[] sessionBytes = "0123456789".getBytes();

    private final byte[] sourceBytes = "9876543210".getBytes();
    private final long source = 33434L;//TODO convert sourceBytes to long

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

    DatagramChannel server = null;

    public Client5() throws Exception {
        path = Paths.get(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
        fileBuffer = new MappedFileBuffer(file);
        long fileSize = file.length();
        fileBuffer = new MappedFileBuffer(file, MappedFileBuffer.MAX_SEGMENT_SIZE, fileSize, MappedFileBuffer.MAX_SEGMENT_SIZE, true, false);
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



        try {
            server = DatagramChannel.open(StandardProtocolFamily.INET6);
            server.bind(null);
            NetworkInterface networkInterface = getNetworkInterface();


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
                        commandByteBuffer.clear();
                        commandPosition = commandByteBuffer.position();
                        fileByteBuffer.position((int)filePosition);
                        fileByteBuffer.limit(fileByteBuffer.capacity());
                        short messageLength = fileDirectBuffer.getShort((int) filePosition, ByteOrder.BIG_ENDIAN);
                        if(messageLength == 0) {
                            active = false;
                        }
                        filePosition += 2;
                        fileByteBuffer.position((int)filePosition);
                        long nextFilePosition = filePosition + messageLength;
                        if(nextFilePosition > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException("Invalid position " + nextFilePosition);
                        }
                        fileByteBuffer.limit((int) nextFilePosition);

                        //write MoldUDP64 Packet
                        long startingCommandPosition = commandPosition;
                        moldUDP64Packet.wrapForEncode(commandDirectBuffer, (int)commandPosition);
                        // Downstream Packet Message Block
                        moldUDP64Packet.downstreamPacketHeader()
                                .putSession(sessionBytes, (int) commandPosition)
                                .sequenceNumber(++sequence)
                                .messageCount(1);//hard code to 1 for now
                        int moldUDP64PacketLength = moldUDP64Packet.size();
                        commandPosition += moldUDP64PacketLength;
                        commandByteBuffer.position((int)commandPosition);

                        //downstream packet message block
                        int streamHeaderSize = streamHeader.size();
                        short totalMessageSize = (short)(streamHeaderSize + messageLength);
                        //messageLength
                        commandDirectBuffer.putShort((int)commandPosition, totalMessageSize, ByteOrder.BIG_ENDIAN);
                        commandPosition += 2;
                        commandByteBuffer.position((int)commandPosition);

                        //streamHeader
                        long streamHeaderPosition = commandPosition;
                        streamHeader.wrap(commandDirectBuffer, (int)streamHeaderPosition, streamHeaderVersion);
                        streamHeader.timestampNanos(System.nanoTime());
                        streamHeader.major((byte)'A');
                        streamHeader.minor((byte)'B');
                        streamHeader.source(source);
                        streamHeader.id(sequence);
                        streamHeader.ref(9999L);
                        commandPosition += streamHeaderSize;
                        commandByteBuffer.position((int)commandPosition);

                        //payload
                        int bytesRead = fileDirectBuffer.getBytes((int) filePosition, commandByteBuffer, messageLength);
//                        int bytesRead = fileDirectBuffer.putBytes((int)filePosition, commandByteBuffer, messageLength);
                        assert (bytesRead == messageLength);
                        byte fileMessageType = fileDirectBuffer.getByte((int)filePosition);
                        byte messageType = commandDirectBuffer.getByte((int)commandPosition);
                        ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);

                        commandPosition += bytesRead;
//                        commandByteBuffer.position((int)startingCommandPosition);
//                        int nextCommandLimit = moldUDP64PacketLength + 2 + messageLength;
//                        commandByteBuffer.limit(nextCommandLimit);
                        commandByteBuffer.position((int)commandPosition);
                        commandByteBuffer.flip();

                        int bytesSent = server.send(commandByteBuffer, group);
                        if((sequence <= 10) || (sequence % 1000000 == 0)) {
                            StringBuilder sb = new StringBuilder();
                            sb
                               .append("[seq=").append(sequence).append("]")
                               .append("[filePosition=").append(filePosition).append("]")
                               .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                               .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                               .append("[messageLength=").append(messageLength).append("]")
                               .append("[bytesSent=").append(bytesSent).append("]")
                               .append("[itchMessageType=").append(itchMessageType).append("]")
                            ;
                            log.info(sb.toString());
                        }

                        filePosition = nextFilePosition;    //2 is for messageLength
                    }
                }
            }
            log.info("finished with seq="+sequence);
        }
        finally {
            server.close();
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

    public static void main(String[] args) throws Exception {
        Client5 client = new Client5();
    }
}
