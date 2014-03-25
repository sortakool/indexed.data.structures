package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.message.nasdaq.SequenceUtility;
import com.rsm.message.nasdaq.itch.v4_1.*;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
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
    public static final String COMMAND_MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    public static final int COMMAND_MULTICAST_PORT = 9000;

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    private final int logModCount = 10000;

    private final MoldUDP64Packet commandMoldUDP64Packet = new MoldUDP64Packet();
    private final StreamHeader commandStreamHeader = new StreamHeader();
    private final int streamHeaderVersion = 1;
    private final byte[] sessionBytes = new byte[DownstreamPacketHeader.sessionLength()];
    private final String sourceString = "client  ";
    private final byte[] sourceBytes = sourceString.getBytes();
    private final long source = 33434L;//TODO convert sourceBytes to long

    Path path;
    File file;
    MappedFileBuffer fileBuffer;
    MappedByteBuffer fileByteBuffer;
    DirectBuffer fileDirectBuffer;
    long filePosition = 0;

//    private int sourceSequence = 0;

    DatagramChannel commandChannel = null;
    ByteBuffer commandByteBuffer;
    DirectBuffer commandDirectBuffer;
    long commandPosition = 0;

    private final EventMoldUDP64Packet eventMoldUDP64Packet = new EventMoldUDP64Packet();
    private final StreamHeader eventStreamHeader = new StreamHeader();
    ByteBuffer eventByteBuffer;
    DirectBuffer eventDirectBuffer;
    int eventPosition = 0;
    long eventSequence = 0;
    MembershipKey eventMembershipKey = null;

    private SequenceUtility sequenceUtility;
    private int eventSequenceIndex;
    private int sourceSequenceIndex;

    public Client5() throws Exception {
        sequenceUtility = new SequenceUtility(2);
        eventSequenceIndex = sequenceUtility.register();
        sourceSequenceIndex = sequenceUtility.register();

        path = Paths.get(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
//        fileBuffer = new MappedFileBuffer(file);
        long fileSize = file.length();
        fileBuffer = new MappedFileBuffer(file, MappedFileBuffer.MAX_SEGMENT_SIZE, fileSize, MappedFileBuffer.MAX_SEGMENT_SIZE, true, false);
        filePosition = fileBuffer.position();
        long capacity = fileBuffer.capacity();
        fileByteBuffer = fileBuffer.buffer(filePosition);
        fileDirectBuffer = new DirectBuffer(fileByteBuffer);

        String fileName = file.getName();
        String[] fileNameParts = fileName.split("\\.");
        String session = fileNameParts[0];
        ByteUtils.fillWithSpaces(sessionBytes);
        System.arraycopy(session.getBytes(), 0, sessionBytes, 0, session.getBytes().length);
        String sessionString = new String(sessionBytes);

        commandByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        DatagramChannel eventChannel;
        eventByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        eventByteBuffer.order(ByteOrder.BIG_ENDIAN);
        eventDirectBuffer = new DirectBuffer(eventByteBuffer);
        eventPosition = 0;

        long eventSequence = 0;

        InetSocketAddress commandGroup = new InetSocketAddress(COMMAND_MULTICAST_IP, COMMAND_MULTICAST_PORT);



        try {
            commandChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
            commandChannel.bind(null);
            NetworkInterface networkInterface = getNetworkInterface();


            int mtu = networkInterface.getMTU();
            log.info("mtu=" + mtu);
            commandChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
            commandChannel.setOption(StandardSocketOptions.SO_SNDBUF, commandByteBuffer.capacity() * 2);
            commandChannel.configureBlocking(false);

            //Create, configure and bind the datagram channel
            eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
            eventChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            InetSocketAddress inetSocketAddress = new InetSocketAddress(EVENT_MULTICAST_PORT);
            eventChannel.bind(inetSocketAddress);
            eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
            eventChannel.configureBlocking(false);

            // join the multicast group on the network interface
            InetAddress eventGroup = InetAddress.getByName(EVENT_MULTICAST_IP);
            eventMembershipKey = eventChannel.join(eventGroup, networkInterface);

            Selector selector = Selector.open();
            SelectionKey commandSelectionKey = commandChannel.register(selector, SelectionKey.OP_WRITE);
            SelectionKey eventSelectionKey = eventChannel.register(selector, SelectionKey.OP_READ);

            String msg = "hello";
//            ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
//            ByteBuffer commandByteBuffer = ByteBuffer.allocateDirect(msg.length());

            boolean active = true;
            StringBuilder sb = new StringBuilder(1024);
            while(active) {
                int selected = selector.selectNow();
                if(selected <= 0) {
                    continue;
                }
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    if (selectionKey.isAcceptable()) {

                    }
                    else if (selectionKey.isConnectable()) {

                    }
                    else if (selectionKey.isReadable()) {
                        DatagramChannel ch = (DatagramChannel)selectionKey.channel();
                        SocketAddress readableSocketAddress = ch.receive(eventByteBuffer);
                        if (readableSocketAddress != null) {
                            eventByteBuffer.flip();
                            int bytesReceived = eventByteBuffer.remaining();

                            //read Event MoldUDP64 Packet
                            int eventPosition = eventByteBuffer.position();
                            int startingEventPosition = eventPosition;
                            eventMoldUDP64Packet.wrapForDecode(eventDirectBuffer, eventPosition, EventMoldUDP64Packet.BLOCK_LENGTH, EventMoldUDP64Packet.SCHEMA_VERSION);
                            eventSequence = eventMoldUDP64Packet.eventSequence();
                            if(!sequenceUtility.equals(eventSequenceIndex, eventSequence)) {
                                sb.setLength(0);
                                sb.append("[expectedEventSequence=").append(sequenceUtility.getSequence(eventSequenceIndex)).append("]")
                                  .append("[eventSequence=").append(eventSequence).append("]")
                                ;
                                log.info(sb.toString());
                                //TODO get missing messages from rewind server
                            }
                            eventMoldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
                            long sourceSequence = eventMoldUDP64Packet.downstreamPacketHeader().sourceSequence();
                            if(!sequenceUtility.equals(sourceSequenceIndex, sourceSequence)) {
                                //there is a major bug if this ever happens
                                sb.setLength(0);
                                sb.append("[expectedSourceSequence").append(sequenceUtility.getSequence(sourceSequenceIndex)).append("]")
                                  .append("[sourceSequence").append(sourceSequence).append("]")
                                ;
                                log.error(sb.toString());
                            }
                            int messageCount = eventMoldUDP64Packet.downstreamPacketHeader().messageCount();
                            eventPosition +=  eventMoldUDP64Packet.size();

                            //downstream packet message block
                            short messageLength = eventDirectBuffer.getShort(eventPosition, ByteOrder.BIG_ENDIAN);
                            eventPosition += 2;
                            eventByteBuffer.position(eventPosition);

                            //streamHeader
                            int streamHeaderPosition = eventPosition;
                            eventStreamHeader.wrap(eventDirectBuffer, eventPosition, streamHeaderVersion);
                            long timestampNanos = eventStreamHeader.timestampNanos();
                            byte major = eventStreamHeader.major();
                            byte minor = eventStreamHeader.minor();
                            long eventSource = eventStreamHeader.source();
                            long id = eventStreamHeader.id();
                            long ref = eventStreamHeader.ref();
                            int streamHeaderSize = eventStreamHeader.size();
                            eventPosition += streamHeaderSize;
                            eventByteBuffer.position(eventPosition);

                            //payload
                            int payloadSize = messageLength - streamHeaderSize;
//                            int bytesRead = eventDirectBuffer.getBytes(eventPosition, eventByteBuffer, payloadSize);
//                            assert (bytesRead == payloadSize);
                            byte messageType = eventDirectBuffer.getByte(eventPosition);
                            ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
                            eventPosition += payloadSize;
                            eventByteBuffer.position(eventPosition);


                            eventSequence = sequenceUtility.adjustSequence(eventSequenceIndex, messageCount);
                            sourceSequence = sequenceUtility.adjustSequence(sourceSequenceIndex, messageCount);

                            if((eventSequence <= 10) || (eventSequence % logModCount == 0)) {
//                            if((sourceSequence >= 0)) {
                                sb.setLength(0);
                                sb.append("event:")
                                        .append("[session=").append(sessionString).append("]")
                                        .append("[eventSequence=").append(eventSequence).append("]")
                                        .append("[source=").append(eventSource).append("]")
                                        .append("[sourceSequence=").append(sourceSequence).append("]")
                                        .append("[eventMoldUDP64PacketLength=").append(eventMoldUDP64Packet.size()).append("]")
                                        .append("[messageLength=").append(messageLength).append("]")
                                        .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                        .append("[payloadSize=").append(payloadSize).append("]")
                                        .append("[bytesReceived=").append(bytesReceived).append("]")
                                        .append("[itchMessageType=").append(itchMessageType).append("]")
                                ;
                                log.info(sb.toString());
                            }

                            if(!eventByteBuffer.hasRemaining()) {
//                                selectionKey.cancel();
                                eventChannel.register(selector, 0);

                                eventByteBuffer.clear();
                                eventPosition = eventByteBuffer.position();
                                commandChannel.register(selector, SelectionKey.OP_WRITE);
                            }
                        }
                    }
                    else if (selectionKey.isWritable()) {
                        commandByteBuffer.clear();
                        commandPosition = commandByteBuffer.position();
                        fileByteBuffer.position((int)filePosition);
                        fileByteBuffer.limit(fileByteBuffer.capacity());
                        short messageLength = fileDirectBuffer.getShort((int) filePosition, ByteOrder.BIG_ENDIAN);
                        if(messageLength == 0) {
                            active = false;
                            break;
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
                        commandMoldUDP64Packet.wrapForEncode(commandDirectBuffer, (int) commandPosition);
                        // Downstream Packet Message Block
                        int messageCount = 1; //hard code to 1 for now
                        commandMoldUDP64Packet.downstreamPacketHeader()
                                .putSession(sessionBytes, (int) commandPosition)
                                .sourceSequence(sequenceUtility.getSequence(sourceSequenceIndex))
                                .messageCount(messageCount);
                        int moldUDP64PacketLength = commandMoldUDP64Packet.size();
                        commandPosition += moldUDP64PacketLength;
                        commandByteBuffer.position((int)commandPosition);

                        //downstream packet message block
                        int streamHeaderSize = commandStreamHeader.size();
                        short totalMessageSize = (short)(streamHeaderSize + messageLength);
                        //messageLength
                        commandDirectBuffer.putShort((int)commandPosition, totalMessageSize, ByteOrder.BIG_ENDIAN);
                        commandPosition += 2;
                        commandByteBuffer.position((int)commandPosition);

                        //streamHeader
                        long streamHeaderPosition = commandPosition;
                        commandStreamHeader.wrap(commandDirectBuffer, (int) streamHeaderPosition, streamHeaderVersion);
                        commandStreamHeader.timestampNanos(System.nanoTime());
                        commandStreamHeader.major((byte) 'A');
                        commandStreamHeader.minor((byte) 'B');
                        commandStreamHeader.source(source);
                        commandStreamHeader.id(sequenceUtility.getSequence(sourceSequenceIndex));
                        commandStreamHeader.ref(9999L);
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

                        int bytesSent = commandChannel.send(commandByteBuffer, commandGroup);
                        if((sequenceUtility.getSequence(sourceSequenceIndex) <= 10) || (sequenceUtility.getSequence(sourceSequenceIndex) % logModCount == 0)) {
                            sb.setLength(0);
                            sb
                               .append("command:")
                               .append("[session=").append(sessionString).append("]")
                               .append("[sourceSequence=").append(sequenceUtility.getSequence(sourceSequenceIndex)).append("]")
                               .append("[source=").append(source).append("]")
                               .append("[filePosition=").append(filePosition).append("]")
                               .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                               .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                               .append("[messageLength=").append(totalMessageSize).append("]")
                               .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                               .append("[payloadSize=").append(messageLength).append("]")
                               .append("[bytesSent=").append(bytesSent).append("]")
                               .append("[itchMessageType=").append(itchMessageType).append("]")
                            ;
                            log.info(sb.toString());
                        }

                        filePosition = nextFilePosition;

                        if(!commandByteBuffer.hasRemaining()) {
//                            selectionKey.cancel();
                            commandChannel.register(selector, 0);

                            commandByteBuffer.clear();
                            commandPosition = commandByteBuffer.position();
                            eventChannel.register(selector, SelectionKey.OP_READ);
                        }
                    }
                }
            }
            log.info("finished with seq="+ sequenceUtility.getSequence(sourceSequenceIndex));
        }
        finally {
            commandChannel.close();
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
