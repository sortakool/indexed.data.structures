package com.rsm.servers;

import com.rsm.message.nasdaq.SequenceUtility;
import com.rsm.message.nasdaq.itch.v4_1.*;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUtils;
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
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Created by rmanaloto on 3/19/14.
 */
public class Sequencer2 {

    private static final Logger log = LogManager.getLogger(Sequencer2.class);

    public static final String COMMAND_MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    public static final int COMMAND_MULTICAST_PORT = 9000;

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    private final int logModCount = 100_000;

    DatagramChannel commandChannel = null;
    MembershipKey commandMembershipKey = null;

    DatagramChannel eventChannel = null;

    ByteBuffer commandByteBuffer;
    DirectBuffer commandDirectBuffer;
    int commandPosition = 0;


    private final MoldUDP64Packet commandMoldUDP64Packet = new MoldUDP64Packet();
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
//    long eventSequence = 0;

    private SequenceUtility sequenceUtility;
    private int eventSequenceIndex;
    private int sourceSequenceIndex;

    public Sequencer2() throws Exception {
        sequenceUtility = new SequenceUtility(2);
        eventSequenceIndex = sequenceUtility.register();
        sourceSequenceIndex = sequenceUtility.register();

        commandByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        eventByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        eventByteBuffer.order(ByteOrder.BIG_ENDIAN);
        eventDirectBuffer = new DirectBuffer(eventByteBuffer);
        eventPosition = 0;

        Selector selector = Selector.open();

        NetworkInterface networkInterface = getNetworkInterface();

        //Create, configure and bind the datagram channel
        commandChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        commandChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(COMMAND_MULTICAST_PORT);
        commandChannel.bind(inetSocketAddress);
        commandChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        // join the multicast group on the network interface
        InetAddress commandGroup = InetAddress.getByName(COMMAND_MULTICAST_IP);
        commandMembershipKey = commandChannel.join(commandGroup, networkInterface);

        InetSocketAddress eventGroup = new InetSocketAddress(EVENT_MULTICAST_IP, EVENT_MULTICAST_PORT);
        eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        eventChannel.bind(null);
        eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
        eventChannel.setOption(StandardSocketOptions.SO_SNDBUF, eventByteBuffer.capacity()*2);
        eventChannel.configureBlocking(false);
        final SelectionKey writableSelectionKey = eventChannel.register(selector, 0);

        //register socket with selector
        // register socket with Selector

        commandChannel.configureBlocking(false);

        final SelectionKey readableSelectionKey = commandChannel.register(selector, SelectionKey.OP_READ);

        boolean active = true;
        StringBuilder sb = new StringBuilder(1024);
        while(active) {
            int updated = selector.selectNow();
            if (updated > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey selectionKey = iter.next();
                    iter.remove();

                    if(selectionKey.isReadable()) {
                        DatagramChannel ch = (DatagramChannel)selectionKey.channel();
                        SocketAddress readableSocketAddress = ch.receive(commandByteBuffer);
                        if (readableSocketAddress != null) {
                            commandByteBuffer.flip();
                            if(commandByteBuffer.hasRemaining()) {

                                //read MoldUDP64 Packet
                                int commandPosition = commandByteBuffer.position();
                                int startingCommandPosition = commandPosition;
                                commandMoldUDP64Packet.wrapForDecode(commandDirectBuffer, commandPosition, MoldUDP64Packet.BLOCK_LENGTH, MoldUDP64Packet.SCHEMA_VERSION);
                                ByteUtils.fillWithSpaces(sessionBytes);
                                commandMoldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
                                long sourceSequence = commandMoldUDP64Packet.downstreamPacketHeader().sourceSequence();
                                if(!sequenceUtility.equals(sourceSequenceIndex, sourceSequence)) {
                                    //there is a major bug if this ever happens
                                    sb.setLength(0);
                                    sb.append("[expectedSourceSequence").append(sequenceUtility.getSequence(sourceSequenceIndex)).append("]")
                                            .append("[sourceSequence").append(sourceSequence).append("]")
                                    ;
                                    log.error(sb.toString());
                                }
                                int messageCount = commandMoldUDP64Packet.downstreamPacketHeader().messageCount();
                                sequenceUtility.adjustSequence(sourceSequenceIndex, messageCount);
                                int moldUDP64PacketLength = commandMoldUDP64Packet.size();
                                commandPosition += moldUDP64PacketLength;
                                commandByteBuffer.position(commandPosition);

                                long eventSequence = sequenceUtility.getSequence(eventSequenceIndex);
                                eventMoldUDP64Packet.wrapForEncode(eventDirectBuffer, eventPosition);//, EventMoldUDP64Packet.BLOCK_LENGTH, EventMoldUDP64Packet.SCHEMA_VERSION);
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
                                eventByteBuffer.position(eventPosition);

                                //payload
                                int payloadSize = messageLength - streamHeaderSize;
                                int bytesRead = commandDirectBuffer.getBytes(commandPosition, eventByteBuffer, payloadSize);
                                assert (bytesRead == payloadSize);
                                byte messageType = commandDirectBuffer.getByte(commandPosition);
                                final byte eventMessageType = eventDirectBuffer.getByte(eventPosition);
                                assert(messageType == eventMessageType);
                                ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
                                commandPosition += payloadSize;
                                commandByteBuffer.position(commandPosition);
                                eventPosition += payloadSize;
                                eventByteBuffer.position(eventPosition);

//                            log.info("[seq="+sourceSequence+"][commandPosition="+commandPosition+"][messageLength="+messageLength+"]");
                                if((eventSequence <= 10) || (eventSequence % logModCount == 0)) {
//                            if((sourceSequence >= 0)) {
                                    sb.setLength(0);
                                    sb.append("command:")
                                            .append("[sequence=").append(eventSequence).append("]")
                                            .append("[source=").append(source).append("]")
                                            .append("[sourceSequence=").append(sourceSequence).append("]")
                                            .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                            .append("[messageLength=").append(messageLength).append("]")
                                            .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                            .append("[payloadSize=").append(payloadSize).append("]")
                                            .append("[itchMessageType=").append(itchMessageType).append("]")
                                    ;
                                    log.info(sb.toString());
                                }
                                if(!commandByteBuffer.hasRemaining()) {
//                                selectionKey.cancel();
//                                commandChannel.register(selector, 0);
//                                    readableSelectionKey.interestOps(0);
                                    commandByteBuffer.clear();
                                    commandPosition = commandByteBuffer.position();
//                                eventChannel.register(selector, SelectionKey.OP_WRITE);
                                    writableSelectionKey.interestOps(SelectionKey.OP_WRITE);
                                }
//                            else {
//                                commandByteBuffer.compact();
//                            }
                            }
                        }
                    }
                    else if(selectionKey.isWritable()) {
                        int eventLimit = eventPosition;
                        eventPosition = eventByteBuffer.position();
                        eventByteBuffer.flip();
                        int eventBytesSent = eventChannel.send(eventByteBuffer, eventGroup);
                        //                            eventSequence++;
                        sequenceUtility.incrementSequence(eventSequenceIndex);

                        long eventSequence = sequenceUtility.getSequence(eventSequenceIndex);
                        if((eventSequence <= 10) || (eventSequence % logModCount == 0)) {
                            sb.setLength(0);
                            sb.append("event:")
                                    .append("[sequence=").append(eventSequence).append("]")
                                    .append("[eventBytesSent=").append(eventBytesSent).append("]")
                            ;
                            log.info(sb.toString());
                        }
                        if(!eventByteBuffer.hasRemaining()){
//                            selectionKey.cancel();
//                            eventChannel.register(selector, 0);
                            writableSelectionKey.interestOps(0);
                            eventByteBuffer.clear();
                            eventPosition = eventByteBuffer.position();
//                            commandChannel.register(selector, SelectionKey.OP_READ);
                            readableSelectionKey.interestOps(SelectionKey.OP_READ);
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
