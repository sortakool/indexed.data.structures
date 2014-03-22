package com.rsm.servers;

import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import com.rsm.message.nasdaq.itch.v4_1.StreamHeader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Created by rmanaloto on 3/19/14.
 */
public class Sequencer2 {

    private static final Logger log = LogManager.getLogger(Sequencer2.class);

    String MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    int MULTICAST_PORT = 9999;

    DatagramChannel server = null;
    MembershipKey key = null;

    ByteBuffer commandByteBuffer;
    DirectBuffer commandDirectBuffer;
    long commandPosition = 0;


    private final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    private final byte[] sessionBytes = new byte[10];
    private final byte[] payloadBytes = new byte[1024];

    private final StreamHeader streamHeader = new StreamHeader();
    private final int streamHeaderVersion = 1;

    public Sequencer2() throws Exception {
        commandByteBuffer = ByteBuffer.allocateDirect(2048);
//        commandByteBuffer = ByteBuffer.allocate(2048);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        NetworkInterface networkInterface = getNetworkInterface();

        //Create, configure and bind the datagram channel
        server = DatagramChannel.open(StandardProtocolFamily.INET6);
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(MULTICAST_PORT);
        server.bind(inetSocketAddress);
        server.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        // join the multicast group on the network interface
        InetAddress group = InetAddress.getByName(MULTICAST_IP);
        key = server.join(group, networkInterface);

        //register socket with selector
        // register socket with Selector
        Selector sel = Selector.open();
        server.configureBlocking(false);
        server.register(sel, SelectionKey.OP_READ);
        boolean active = true;
        while(active) {
            int updated = sel.selectNow();
            if (updated > 0) {
                Iterator<SelectionKey> iter = sel.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey sk = iter.next();
                    iter.remove();

                    if(sk.isReadable()) {
                        DatagramChannel ch = (DatagramChannel)sk.channel();
                        SocketAddress sa = ch.receive(commandByteBuffer);
                        if (sa != null) {
                            commandByteBuffer.flip();

                            //read MoldUDP64 Packet
                            int commandPosition = commandByteBuffer.position();
                            int startingCommandPosition = commandPosition;
                            moldUDP64Packet.wrapForDecode(commandDirectBuffer, commandPosition, MoldUDP64Packet.BLOCK_LENGTH, MoldUDP64Packet.SCHEMA_VERSION);
                            Arrays.fill(sessionBytes, (byte) ' ');
                            moldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
                            long sequence = moldUDP64Packet.downstreamPacketHeader().sequenceNumber();
                            int messageCount = moldUDP64Packet.downstreamPacketHeader().messageCount();
                            int moldUDP64PacketLength = moldUDP64Packet.size();
                            commandPosition += moldUDP64PacketLength;
                            commandByteBuffer.position(commandPosition);

                            //downstream packet message block
                            short messageLength = commandDirectBuffer.getShort(commandPosition, ByteOrder.BIG_ENDIAN);
                            commandPosition += 2;
                            commandByteBuffer.position(commandPosition);

                            //streamHeader
                            int streamHeaderPosition = commandPosition;
                            streamHeader.wrap(commandDirectBuffer, commandPosition, streamHeaderVersion);
                            long timestampNanos = streamHeader.timestampNanos();
                            byte major = streamHeader.major();
                            byte minor = streamHeader.minor();
                            long source = streamHeader.source();
                            long id = streamHeader.id();
                            long ref = streamHeader.ref();
                            int streamHeaderSize = streamHeader.size();
                            commandPosition += streamHeaderSize;
                            commandByteBuffer.position((int)commandPosition);

                            //payload
                            int payloadSize = messageLength - streamHeaderSize;
                            int bytesRead = commandDirectBuffer.getBytes(commandPosition, payloadBytes, 0, payloadSize);
                            assert (bytesRead == payloadSize);
                            byte messageType = commandDirectBuffer.getByte(commandPosition);
                            commandPosition += payloadSize;
                            commandByteBuffer.position(commandPosition);

//                            log.info("[seq="+sequence+"][commandPosition="+commandPosition+"][messageLength="+messageLength+"]");
                            if((sequence <= 10) || (sequence % 1000000 == 0)) {
                                StringBuilder sb = new StringBuilder();
                                sb
                                        .append("[seq=").append(sequence).append("]")
                                        .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                        .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                        .append("[messageLength=").append(messageLength).append("]")
                                        .append("[bytesRead=").append(bytesRead).append("]")
                                        .append("[messageType=").append((char)messageType).append("]")
                                ;
                                log.info(sb.toString());
                            }


//                            printDatagram(sa, commandByteBuffer);
//                            commandByteBuffer.rewind();
//                            commandByteBuffer.limit(commandByteBuffer.capacity());
                            commandByteBuffer.compact();
                        }
                    }
                }
            }
        }
    }

    static void printDatagram(SocketAddress sa, ByteBuffer buf) {
        System.out.format("-- datagram from %s --\n",
                ((InetSocketAddress) sa).getAddress().getHostAddress());
        System.out.println(Charset.defaultCharset().decode(buf));
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
        Sequencer2 sequencer = new Sequencer2();
    }
}
