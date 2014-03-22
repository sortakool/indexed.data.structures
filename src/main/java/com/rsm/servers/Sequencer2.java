package com.rsm.servers;

import com.rsm.message.nasdaq.itch.v4_1.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;
import uk.co.real_logic.sbe.util.BitUtil;

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

    String COMMAND_MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    int COMMAND_MULTICAST_PORT = 9000;

    String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    int EVENT_MULTICAST_PORT = 9001;

    DatagramChannel server = null;
    MembershipKey key = null;

    ByteBuffer commandByteBuffer;
    DirectBuffer commandDirectBuffer;
    int commandPosition = 0;


    private final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    private final StreamHeader commandStreamHeader = new StreamHeader();
    private final int streamHeaderVersion = 1;
    private final byte[] sessionBytes = new byte[DownstreamPacketHeader.sessionLength()];
    private final byte[] payloadBytes = new byte[1024];
    private final byte[] sourceBytes = new byte[BitUtil.SIZE_OF_LONG];

    private final EventMoldUDP64Packet eventMoldUDP64Packet = new EventMoldUDP64Packet();
    private final StreamHeader eventStreamHeader = new StreamHeader();
    ByteBuffer eventByteBuffer;
    DirectBuffer eventDirectBuffer;
    int eventPosition = 0;
    long eventSequence = 0;

    public Sequencer2() throws Exception {
        commandByteBuffer = ByteBuffer.allocateDirect(2048);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        eventByteBuffer = ByteBuffer.allocateDirect(2048);
        eventByteBuffer.order(ByteOrder.BIG_ENDIAN);
        eventDirectBuffer = new DirectBuffer(eventByteBuffer);
        eventPosition = 0;

        NetworkInterface networkInterface = getNetworkInterface();

        //Create, configure and bind the datagram channel
        server = DatagramChannel.open(StandardProtocolFamily.INET6);
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(COMMAND_MULTICAST_PORT);
        server.bind(inetSocketAddress);
        server.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        // join the multicast group on the network interface
        InetAddress group = InetAddress.getByName(COMMAND_MULTICAST_IP);
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

                            eventSequence++;
                            
                            //read MoldUDP64 Packet
                            int commandPosition = commandByteBuffer.position();
                            int startingCommandPosition = commandPosition;
                            moldUDP64Packet.wrapForDecode(commandDirectBuffer, commandPosition, MoldUDP64Packet.BLOCK_LENGTH, MoldUDP64Packet.SCHEMA_VERSION);
                            Arrays.fill(sessionBytes, (byte) ' ');
                            moldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
                            long sourceSequence = moldUDP64Packet.downstreamPacketHeader().sourceSequence();
                            int messageCount = moldUDP64Packet.downstreamPacketHeader().messageCount();
                            int moldUDP64PacketLength = moldUDP64Packet.size();
                            commandPosition += moldUDP64PacketLength;
                            commandByteBuffer.position(commandPosition);

                            eventMoldUDP64Packet.wrapForDecode(eventDirectBuffer, eventPosition, EventMoldUDP64Packet.BLOCK_LENGTH, EventMoldUDP64Packet.SCHEMA_VERSION);
                            eventMoldUDP64Packet.eventSequence(eventSequence);
                            eventMoldUDP64Packet.downstreamPacketHeader().putSession(sessionBytes, 0);
                            eventMoldUDP64Packet.downstreamPacketHeader().sourceSequence(sourceSequence);
                            eventMoldUDP64Packet.downstreamPacketHeader().messageCount(1);
                            eventPosition +=  eventMoldUDP64Packet.size();


                            //downstream packet message block
                            short messageLength = commandDirectBuffer.getShort(commandPosition, ByteOrder.BIG_ENDIAN);
                            commandPosition += 2;
                            commandByteBuffer.position(commandPosition);

                            eventDirectBuffer.putShort(eventPosition, messageLength, ByteOrder.BIG_ENDIAN);
                            eventPosition += 2;

                            //streamHeader
                            int streamHeaderPosition = commandPosition;
                            commandStreamHeader.wrap(commandDirectBuffer, commandPosition, streamHeaderVersion);
                            long timestampNanos = commandStreamHeader.timestampNanos();
                            byte major = commandStreamHeader.major();
                            byte minor = commandStreamHeader.minor();
                            long source = commandStreamHeader.source();
                            long id = commandStreamHeader.id();
                            long ref = commandStreamHeader.ref();
                            int streamHeaderSize = commandStreamHeader.size();
                            commandPosition += streamHeaderSize;
                            commandByteBuffer.position((int)commandPosition);

                            eventStreamHeader.wrap(eventDirectBuffer, eventPosition, streamHeaderVersion);
                            eventStreamHeader.timestampNanos(timestampNanos);
                            eventStreamHeader.major(major);
                            eventStreamHeader.minor(minor);
                            eventStreamHeader.source(source);
                            eventStreamHeader.id(id);
                            eventStreamHeader.ref(ref);
                            eventPosition += streamHeaderSize;
//                            eventByteBuffer.position(eventPosition);

                            //payload
                            int payloadSize = messageLength - streamHeaderSize;
                            int bytesRead = commandDirectBuffer.getBytes(eventPosition, eventByteBuffer, payloadSize);
                            assert (bytesRead == payloadSize);
                            byte messageType = commandDirectBuffer.getByte(commandPosition);
                            ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
                            commandPosition += payloadSize;
                            eventPosition += payloadSize;
                            commandByteBuffer.position(commandPosition);

//                            log.info("[seq="+sourceSequence+"][commandPosition="+commandPosition+"][messageLength="+messageLength+"]");
//                            if((sourceSequence <= 10) || (sourceSequence % 1000000 == 0)) {
                            if((sourceSequence >= 0)) {
                                StringBuilder sb = new StringBuilder();
                                sb
                                        .append("[source=").append(source).append("]")
                                        .append("[sourceSequence=").append(sourceSequence).append("]")
                                        .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                        .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                        .append("[messageLength=").append(messageLength).append("]")
                                        .append("[bytesRead=").append(bytesRead).append("]")
                                        .append("[itchMessageType=").append(itchMessageType).append("]")
                                ;
                                log.info(sb.toString());
                            }
                            commandByteBuffer.compact();
                        }
                    }
                    else if(sk.isWritable()) {
                        int eventLimit = eventPosition;
                        eventByteBuffer.flip();

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
