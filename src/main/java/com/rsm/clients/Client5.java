package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.io.selector.SelectedSelectionKeySet;
import com.rsm.io.selector.SelectorUtil;
import com.rsm.message.nasdaq.SequenceUtility;
import com.rsm.message.nasdaq.binaryfile.BinaryFile;
import com.rsm.message.nasdaq.itch.v4_1.*;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUtils;
import net.openhft.chronicle.ChronicleConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
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

    private final int logModCount = 1_000_000;

    private final MoldUDP64Packet commandMoldUDP64Packet = new MoldUDP64Packet();
    private final StreamHeader commandStreamHeader = new StreamHeader();
    private final int streamHeaderVersion = 1;
    private final byte[] sessionBytes = new byte[DownstreamPacketHeader.sessionLength()];
    private final String sourceString = "client  ";
//    private final byte[] sourceBytes = sourceString.getBytes();
    private final long source;

    private final byte[] commandSourceBytes = new byte[BitUtil.SIZE_OF_LONG];
    private final byte[] eventSourceBytes = new byte[BitUtil.SIZE_OF_LONG];

    Path path;
    File file;
    BinaryFile binaryFile;

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

    private final SelectedSelectionKeySet selectedKeys = new SelectedSelectionKeySet();
    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;

    private int messageCount = 0;

    private final int cutoff = 256;

    long expectedEventSequence = 1;
    long expectedSourceSequence = 1;

    long totalBytesSent = 0;
    long totalBytesReceived = 0;

    public Client5() throws Exception {
        source = ByteUtils.getLongBigEndian(sourceString.getBytes(), 0);
        ByteUtils.fillWithSpaces(commandSourceBytes);
        ByteUtils.putLongBigEndian(commandSourceBytes, 0, source);

        sequenceUtility = new SequenceUtility(2);
        eventSequenceIndex = sequenceUtility.register();
        sourceSequenceIndex = sequenceUtility.register();

        path = Paths.get(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
        String absolutePath = file.getAbsolutePath();
        long fileSize = file.length();
        int dataBlockSize = ChronicleConfig.SMALL.dataBlockSize();
        binaryFile = new BinaryFile(absolutePath, dataBlockSize, fileSize, dataBlockSize, ByteOrder.BIG_ENDIAN);


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
            commandChannel.setOption(StandardSocketOptions.SO_SNDBUF, MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE * 10);
            commandChannel.configureBlocking(false);

            //Create, configure and bind the datagram channel
            eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
            eventChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            InetSocketAddress inetSocketAddress = new InetSocketAddress(EVENT_MULTICAST_PORT);
            eventChannel.bind(inetSocketAddress);
            eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
            eventChannel.setOption(StandardSocketOptions.SO_RCVBUF, MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*10);
            eventChannel.configureBlocking(false);

            // join the multicast group on the network interface
            InetAddress eventGroup = InetAddress.getByName(EVENT_MULTICAST_IP);
            eventMembershipKey = eventChannel.join(eventGroup, networkInterface);

            Selector selector = Selector.open();
            SelectorUtil.optimizeSelector(selector, selectedKeys);

            SelectionKey commandSelectionKey = commandChannel.register(selector, SelectionKey.OP_WRITE, commandByteBuffer);
            SelectionKey eventSelectionKey = eventChannel.register(selector, SelectionKey.OP_READ, eventByteBuffer);

            printOptions(commandChannel, "command ", "");
            printOptions(eventChannel, "event ", "");

            boolean active = true;
            StringBuilder sb = new StringBuilder(1024);
            while(active  && binaryFile.hasNext()) {
                int selected = selector.selectNow();
                final int size = selectedKeys.size();
                assert(selected == size);
                SelectionKey[] selectionKeys = selectedKeys.flip();

                for (int i = 0; i<selected; i ++) {
                    SelectionKey selectionKey = selectionKeys[i];
                    final Object a = selectionKey.attachment();


                    if (selectionKey.isAcceptable()) {

                    }
                    else if (selectionKey.isConnectable()) {

                    }
                    else if (selectionKey.isReadable()) {
                        DatagramChannel ch = (DatagramChannel)selectionKey.channel();
                        SocketAddress readableSocketAddress = ch.receive(eventByteBuffer);
                        if (readableSocketAddress != null) {
                            eventByteBuffer.flip();
                            if(eventByteBuffer.hasRemaining()) {
                                int bytesReceived = eventByteBuffer.remaining();
                                totalBytesReceived += bytesReceived;

                                //read Event MoldUDP64 Packet
                                int eventPosition = eventByteBuffer.position();
                                int startingEventPosition = eventPosition;
                                eventMoldUDP64Packet.wrapForDecode(eventDirectBuffer, eventPosition, EventMoldUDP64Packet.BLOCK_LENGTH, EventMoldUDP64Packet.SCHEMA_VERSION);
                                eventSequence = eventMoldUDP64Packet.eventSequence();
                                if(expectedEventSequence != eventSequence) {
                                    sb.setLength(0);
                                    sb.append("[eventSequence=").append(eventSequence).append("]")
                                      .append("[expectedEventSequence=").append(expectedEventSequence).append("]")
                                      .append("[currentEventSequence=").append(sequenceUtility.getSequence(eventSequenceIndex)).append("]")
                                    ;
                                    log.info(sb.toString());
                                    //TODO get missing messages from rewind server
                                }
                                eventMoldUDP64Packet.downstreamPacketHeader().getSession(sessionBytes, 0);
                                long sourceSequence = eventMoldUDP64Packet.downstreamPacketHeader().sourceSequence();
                                int messageCount = eventMoldUDP64Packet.downstreamPacketHeader().messageCount();
                                eventPosition +=  eventMoldUDP64Packet.size();

                                //now each individual message
                                for(int j=0; j<messageCount;j++) {
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
                                    ByteUtils.fillWithSpaces(eventSourceBytes);
                                    ByteUtils.putLongBigEndian(eventSourceBytes, 0, eventSource);
                                    long id = eventStreamHeader.id();
                                    long ref = eventStreamHeader.ref();
                                    int streamHeaderSize = eventStreamHeader.size();
                                    eventPosition += streamHeaderSize;
                                    eventByteBuffer.position(eventPosition);

                                    if(eventSource == source) {
                                        if(expectedSourceSequence != sourceSequence) {
                                            //there is a major bug if this ever happens
                                            sb.setLength(0);
                                            sb.append("[sourceSequence=").append(sourceSequence).append("]")
                                              .append("[expectedSourceSequence=").append(expectedSourceSequence).append("]")
                                              .append("[currentSourceSequence=").append(sequenceUtility.getSequence(sourceSequenceIndex)).append("]")
                                            ;
                                            log.error(sb.toString());
                                        }
                                        else {
                                            //eventually cancel the timer waiting for this event to come through
                                        }
                                    }
                                    else {

                                    }

                                    //payload
                                    int payloadSize = messageLength - streamHeaderSize;
//                            int bytesRead = eventDirectBuffer.getBytes(eventPosition, eventByteBuffer, payloadSize);
//                            assert (bytesRead == payloadSize);
                                    byte messageType = eventDirectBuffer.getByte(eventPosition);
                                    ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
                                    eventPosition += payloadSize;
                                    eventByteBuffer.position(eventPosition);

                                    if((eventSequence <= 10) || (eventSequence % logModCount == 0)) {
//                            if((sourceSequence >= 0)) {
                                        sb.setLength(0);
                                        sb.append("event:")
                                                .append("[session=").append(sessionString).append("]")
                                                .append("[eventSequence=").append(eventSequence).append("]")
                                                .append("[source=").append(new String(eventSourceBytes)).append("]")
                                                .append("[sourceSequence=").append(sourceSequence).append("]")
                                                .append("[eventMoldUDP64PacketLength=").append(eventMoldUDP64Packet.size()).append("]")
                                                .append("[messageLength=").append(messageLength).append("]")
                                                .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                                .append("[payloadSize=").append(payloadSize).append("]")
                                                .append("[bytesReceived=").append(bytesReceived).append("]")
                                                .append("[totalBytesReceived=").append(totalBytesReceived).append("]")
                                                .append("[itchMessageType=").append(itchMessageType).append("]")
                                        ;
                                        log.info(sb.toString());
                                    }

                                    eventSequence = sequenceUtility.incrementSequence(eventSequenceIndex);
//                                    sourceSequence = sequenceUtility.incrementSequence(sourceSequenceIndex);
                                    expectedEventSequence++;
                                }

                                if(!eventByteBuffer.hasRemaining()) {
//                                selectionKey.cancel();
//                                eventChannel.register(selector, 0);
//                                    eventSelectionKey.interestOps(0);

                                    eventByteBuffer.clear();
                                    eventPosition = eventByteBuffer.position();
                                    commandSelectionKey.interestOps(SelectionKey.OP_WRITE);
                                }
                            }
                        }
                    }
                    else if (selectionKey.isWritable()) {
                        commandByteBuffer.clear();
                        commandPosition = commandByteBuffer.position();

                        long startingCommandSequence = sequenceUtility.getSequence(sourceSequenceIndex);

                        //write MoldUDP64 Packet
                        long startingCommandPosition = commandPosition;
                        commandMoldUDP64Packet.wrapForEncode(commandDirectBuffer, (int) commandPosition);
                        // Downstream Packet Message Block
                        messageCount = 0;
                        commandMoldUDP64Packet.downstreamPacketHeader()
                                .putSession(sessionBytes, (int) commandPosition)
                                .sourceSequence(startingCommandSequence)
                                .messageCount(messageCount);
                        int moldUDP64PacketLength = commandMoldUDP64Packet.size();
                        commandPosition += moldUDP64PacketLength;
                        commandByteBuffer.position((int)commandPosition);

                        long currentCommandSequence = sequenceUtility.getSequence(sourceSequenceIndex);
                        expectedSourceSequence = currentCommandSequence;

                        //now each individual message
                        while(binaryFile.hasNext()) {
                            long startingMessageCommandPosition = commandPosition;
                            short messageLength = binaryFile.getCurrentMessageLength();
                            if(messageLength == 0) {
                                active = false;
                                break;
                            }

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
                            final long id = startingCommandSequence + messageCount;
                            commandStreamHeader.id(id);
                            commandStreamHeader.ref(9999L);
                            commandPosition += streamHeaderSize;
                            commandByteBuffer.position((int)commandPosition);

                            //payload
                            int bytesRead = binaryFile.next(commandByteBuffer);
                            assert (bytesRead == messageLength);
                            byte messageType = commandDirectBuffer.getByte((int)commandPosition);
                            ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);

                            commandPosition += bytesRead;
                            commandByteBuffer.position((int)commandPosition);

                            if((currentCommandSequence <= 1000) || (currentCommandSequence % logModCount == 0)) {
                                sb.setLength(0);
                                sb
                                        .append("command:")
                                        .append("[session=").append(sessionString).append("]")
                                        .append("[startingSourceSequence=").append(startingCommandSequence).append("]")
                                        .append("[currentCommandSequence=").append(currentCommandSequence).append("]")
                                        .append("[messageCount=").append(messageCount).append("]")
                                        .append("[source=").append(new String(commandSourceBytes)).append("]")
                                        .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                        .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                        .append("[messageLength=").append(totalMessageSize).append("]")
                                        .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                        .append("[payloadSize=").append(messageLength).append("]")
                                        .append("[itchMessageType=").append(itchMessageType).append("]")
                                        .append("[startingCommandPosition=").append(startingCommandPosition).append("]")
                                        .append("[startingMessageCommandPosition=").append(startingMessageCommandPosition).append("]")
                                        .append("[commandPosition=").append(commandPosition).append("]")
                                ;
                                log.info(sb.toString());
                            }

                            messageCount++;
                            currentCommandSequence = sequenceUtility.incrementSequence(sourceSequenceIndex);

                            final int diff = MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE - commandByteBuffer.position();
                            if(diff <= cutoff) {
                                //set message count
                                commandMoldUDP64Packet.wrapForEncode(commandDirectBuffer, (int)startingCommandPosition);
                                commandMoldUDP64Packet.downstreamPacketHeader().messageCount(messageCount);

                                commandByteBuffer.flip();
                                int bytesSent = commandChannel.send(commandByteBuffer, commandGroup);
                                totalBytesSent += bytesSent;
                                final long endingCommandSequence = sequenceUtility.getSequence(sourceSequenceIndex);
                                if((currentCommandSequence <= 1000) || (currentCommandSequence % logModCount == 0)) {
                                    sb.setLength(0);
                                    sb
                                            .append("command:")
                                            .append("[session=").append(sessionString).append("]")
                                            .append("[startingSourceSequence=").append(startingCommandSequence).append("]")
                                            .append("[currentCommandSequence=").append(currentCommandSequence).append("]")
                                            .append("[endingCommandSequence=").append(endingCommandSequence).append("]")
                                            .append("[messageCount=").append(messageCount).append("]")
                                            .append("[source=").append(new String(commandSourceBytes)).append("]")
                                            .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                            .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                            .append("[messageLength=").append(totalMessageSize).append("]")
                                            .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                            .append("[payloadSize=").append(messageLength).append("]")
                                            .append("[bytesSent=").append(bytesSent).append("]")
                                            .append("[totalBytesSent=").append(totalBytesSent).append("]")
                                            .append("[itchMessageType=").append(itchMessageType).append("]")
                                    ;
                                    log.info(sb.toString());
                                }

                                if(!commandByteBuffer.hasRemaining()) {
                                    commandSelectionKey.interestOps(0);

                                    commandByteBuffer.clear();
                                    commandPosition = commandByteBuffer.position();
                                    break;
                                }
                            }
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
            log.info(nextNetworkInterface+": [supportsMulticast="+nextNetworkInterface.supportsMulticast()+"][virtual="+nextNetworkInterface.isVirtual()+"]");
            if(nextNetworkInterface.supportsMulticast()) {
                networkInterface = nextNetworkInterface;
//                break;
            }
        }
        return networkInterface;
    }

    private static void printOptions(NetworkChannel channel, String prefix, String suffix) throws IOException {
        log.info(prefix + channel.getClass().getSimpleName() + suffix + " supports:");
        for (SocketOption<?> option : channel.supportedOptions()) {
            log.info("\t" + option.name() + ": " + channel.getOption(option));
        }
    }

    public static void main(String[] args) throws Exception {
        Client5 client = new Client5();
    }
}
