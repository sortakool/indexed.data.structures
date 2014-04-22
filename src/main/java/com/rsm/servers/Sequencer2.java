package com.rsm.servers;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.buffer.NativeMappedMemory;
import com.rsm.io.selector.SelectedSelectionKeySet;
import com.rsm.io.selector.SelectorUtil;
import com.rsm.message.nasdaq.SequenceUtility;
import com.rsm.message.nasdaq.binaryfile.IndexedBinaryFile;
import com.rsm.message.nasdaq.binaryfile.IndexedBinaryFileConfig;
import com.rsm.message.nasdaq.itch.v4_1.*;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import com.rsm.util.ByteUnit;
import com.rsm.util.ByteUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;
import uk.co.real_logic.sbe.util.BitUtil;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Enumeration;

/**
 * Created by rmanaloto on 3/19/14.
 */
public class Sequencer2 {

    private static final Logger log = LogManager.getLogger(Sequencer2.class);

    public static final String COMMAND_MULTICAST_IP = "FF02:0:0:0:0:0:0:3";
    public static final int COMMAND_MULTICAST_PORT = 9000;

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    private final int logModCount = 5_000_000;

    DatagramChannel commandChannel = null;
    MembershipKey commandMembershipKey = null;

    DatagramChannel eventChannel = null;

    ByteBuffer commandBuffer;
    NativeMappedMemory commandByteBuffer;
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
    ByteBuffer eventBuffer;
    NativeMappedMemory eventByteBuffer;
    DirectBuffer eventDirectBuffer;
    long eventPosition = 0;
//    long eventSequence = 0;

    private SequenceUtility sequenceUtility;
    private int eventSequenceIndex;
    private int sourceSequenceIndex;

    private final IndexedBinaryFile indexedBinaryFile;

    private final byte[] commandSourceBytes = new byte[BitUtil.SIZE_OF_LONG];
    private final byte[] eventSourceBytes = new byte[BitUtil.SIZE_OF_LONG];

    private final SelectedSelectionKeySet selectedKeys = new SelectedSelectionKeySet();
    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;

    private long currentEventSequence = 1;

    private long totalBytesSent = 0;
    private long totalBytesReceived = 0;

    public Sequencer2(SequencerConfig sequencerConfig) throws Exception {
        ByteUtils.fillWithSpaces(commandSourceBytes);
        ByteUtils.fillWithSpaces(eventSourceBytes);

        indexedBinaryFile = new IndexedBinaryFile((sequencerConfig.getIndexedBinaryFileConfig()));

        sequenceUtility = new SequenceUtility(2);
        eventSequenceIndex = sequenceUtility.register();
        sourceSequenceIndex = sequenceUtility.register();

        commandBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        commandBuffer.order(ByteOrder.BIG_ENDIAN);
        commandByteBuffer = new NativeMappedMemory(commandBuffer);
        commandDirectBuffer = new DirectBuffer(commandBuffer);
        commandPosition = 0;

        eventBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        eventBuffer.order(ByteOrder.BIG_ENDIAN);
        eventByteBuffer = new NativeMappedMemory(eventBuffer);
        eventDirectBuffer = new DirectBuffer(eventBuffer);
        eventPosition = 0;

        Selector selector = Selector.open();
        SelectorUtil.optimizeSelector(selector, selectedKeys);

        NetworkInterface networkInterface = getNetworkInterface();

        //Create, configure and bind the datagram channel
        commandChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        commandChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(COMMAND_MULTICAST_PORT);
        commandChannel.bind(inetSocketAddress);
        commandChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
        commandChannel.setOption(StandardSocketOptions.SO_RCVBUF, MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*10);

        // join the multicast group on the network interface
        InetAddress commandGroup = InetAddress.getByName(COMMAND_MULTICAST_IP);
        commandMembershipKey = commandChannel.join(commandGroup, networkInterface);

        InetSocketAddress eventGroup = new InetSocketAddress(EVENT_MULTICAST_IP, EVENT_MULTICAST_PORT);
        eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        eventChannel.bind(null);
        eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
        eventChannel.setOption(StandardSocketOptions.SO_SNDBUF, MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*10);
        eventChannel.configureBlocking(false);
        final SelectionKey writableSelectionKey = eventChannel.register(selector, 0, eventByteBuffer);

        //register socket with selector
        // register socket with Selector

        commandChannel.configureBlocking(false);

        final SelectionKey readableSelectionKey = commandChannel.register(selector, SelectionKey.OP_READ, commandByteBuffer);

        printOptions(commandChannel, "command ", "");
        printOptions(eventChannel, "event ", "");

        boolean active = true;
        StringBuilder sb = new StringBuilder(1024);
        while(active) {
            cancelledKeys = 0;
            needsToSelectAgain = false;
            final long ioStartTime = System.nanoTime();
            int selected = selector.selectNow();
            final int size = selectedKeys.size();
            assert(selected == size);
            SelectionKey[] selectionKeys = selectedKeys.flip();
//            if(size == 0) {
//                continue;
//            }
//            if ( (size > 0) ) { //&& (size > 0)
                for (int i = 0; i<size; i ++) {
                    SelectionKey selectionKey = selectionKeys[i];

                    final Object a = selectionKey.attachment();

                    if(selectionKey.isReadable()) {
                        DatagramChannel ch = (DatagramChannel)selectionKey.channel();
                        commandBuffer.position((int)commandByteBuffer.position());
                        commandBuffer.limit((int) commandByteBuffer.limit());
                        SocketAddress readableSocketAddress = ch.receive(commandBuffer);
                        commandByteBuffer.position(commandBuffer.position());
                        commandByteBuffer.limit(commandBuffer.limit());
                        if (readableSocketAddress != null) {
                            commandByteBuffer.flip();
                            if(commandByteBuffer.hasRemaining()) {
                                long bytesReceived = commandByteBuffer.remaining();
                                totalBytesReceived += bytesReceived;

                                //read MoldUDP64 Packet
                                long commandPosition = commandByteBuffer.position();
                                long startingCommandPosition = commandPosition;
                                commandMoldUDP64Packet.wrapForDecode(commandDirectBuffer, (int)commandPosition, MoldUDP64Packet.BLOCK_LENGTH, MoldUDP64Packet.SCHEMA_VERSION);
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

                                long eventSequence = sequenceUtility.getSequence(eventSequenceIndex);
                                currentEventSequence = eventSequence;

                                eventMoldUDP64Packet.wrapForEncode(eventDirectBuffer, (int)eventPosition);//, EventMoldUDP64Packet.BLOCK_LENGTH, EventMoldUDP64Packet.SCHEMA_VERSION);
                                eventMoldUDP64Packet.eventSequence(eventSequence);
                                eventMoldUDP64Packet.downstreamPacketHeader().putSession(sessionBytes, 0);
                                eventMoldUDP64Packet.downstreamPacketHeader().sourceSequence(sourceSequence);
                                eventMoldUDP64Packet.downstreamPacketHeader().messageCount(messageCount);
                                eventPosition +=  eventMoldUDP64Packet.size();


                                final long startingSourceSequence = sequenceUtility.getSequence(sourceSequenceIndex);
                                long currentCommandSequence = startingSourceSequence;

                                int moldUDP64PacketLength = commandMoldUDP64Packet.size();
                                commandPosition += moldUDP64PacketLength;
                                commandByteBuffer.position(commandPosition);

                                //now each individual message
                                for(int j=0; j<messageCount;j++) {
                                    //downstream packet message block
                                    short messageLength = commandDirectBuffer.getShort((int)commandPosition, ByteOrder.BIG_ENDIAN);
                                    commandPosition += 2;
                                    commandByteBuffer.position(commandPosition);

                                    eventDirectBuffer.putShort((int)eventPosition, messageLength, ByteOrder.BIG_ENDIAN);
                                    eventPosition += 2;

                                    //streamHeader
                                    long streamHeaderPosition = commandPosition;
                                    commandStreamHeader.wrap(commandDirectBuffer, (int)commandPosition, streamHeaderVersion);
                                    long timestampNanos = commandStreamHeader.timestampNanos();
                                    byte major = commandStreamHeader.major();
                                    byte minor = commandStreamHeader.minor();
                                    long source = commandStreamHeader.source();
                                    long id = commandStreamHeader.id();
                                    long ref = commandStreamHeader.ref();
                                    int streamHeaderSize = commandStreamHeader.size();
                                    commandPosition += streamHeaderSize;
                                    commandByteBuffer.position((int)commandPosition);

                                    ByteUtils.fillWithSpaces(eventSourceBytes);
                                    ByteUtils.putLongBigEndian(eventSourceBytes, 0, source);

                                    long eventStreamHeaderPosition = eventPosition;
                                    eventStreamHeader.wrap(eventDirectBuffer, (int)eventPosition, streamHeaderVersion);
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
                                    eventBuffer.position((int)eventByteBuffer.position());
                                    eventBuffer.limit((int)eventByteBuffer.limit());
                                    int bytesRead = commandDirectBuffer.getBytes((int)commandPosition, eventBuffer, payloadSize);
                                    eventByteBuffer.position(eventBuffer.position());
                                    eventByteBuffer.limit(eventBuffer.limit());
                                    assert (bytesRead == payloadSize);
                                    byte messageType = commandDirectBuffer.getByte((int)commandPosition);
                                    final byte eventMessageType = eventDirectBuffer.getByte((int)eventPosition);
                                    assert(messageType == eventMessageType);
                                    ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);
                                    commandPosition += payloadSize;
                                    commandByteBuffer.position(commandPosition);
                                    eventPosition += payloadSize;
                                    eventByteBuffer.position(eventPosition);

                                    //write event to indexed binary file
                                    long eventByteBufferLimit = eventByteBuffer.limit();
                                    eventByteBuffer.position(eventStreamHeaderPosition);
                                    eventByteBuffer.limit(eventPosition);
//                                    final int remaining = eventByteBuffer.remaining();
                                    short len = (short)(eventStreamHeader.size() + payloadSize);
//                                    assert(remaining == len);
                                    final long startDataMappedFilePosition = indexedBinaryFile.getDataMappedFilePosition();
                                    eventByteBuffer.mark();
                                    final long indexedBinaryFileSequence = indexedBinaryFile.increment(len, eventByteBuffer);
                                    final long endDataMappedFilePosition = indexedBinaryFile.getDataMappedFilePosition();
                                    assert(endDataMappedFilePosition == (startDataMappedFilePosition+len+BitUtil.SIZE_OF_SHORT));
                                    assert(indexedBinaryFileSequence == eventSequence);
//                                    eventByteBuffer.rewind();
                                    eventByteBuffer.limit(eventByteBufferLimit);

                                    if((currentCommandSequence <= 1000) || (currentCommandSequence % logModCount == 0)) {
                                        sb.setLength(0);
                                        sb.append("command:")
                                                .append("[session=").append(new String(sessionBytes)).append("]")
                                                .append("[startingSourceSequence=").append(startingSourceSequence).append("]")
                                                .append("[currentCommandSequence=").append(currentCommandSequence).append("]")
                                                .append("[sequence=").append(eventSequence).append("]")
                                                .append("[source=").append(new String(eventSourceBytes)).append("]")
                                                .append("[sourceSequence=").append(sourceSequence).append("]")
                                                .append("[moldUDP64PacketLength=").append(moldUDP64PacketLength).append("]")
                                                .append("[messageLength=").append(messageLength).append("]")
                                                .append("[streamHeaderSize=").append(streamHeaderSize).append("]")
                                                .append("[payloadSize=").append(payloadSize).append("]")
                                                .append("[bytesReceived=").append(bytesReceived).append("]")
                                                .append("[totalBytesReceived=").append(totalBytesReceived).append("]")
                                                .append("[itchMessageType=").append(itchMessageType).append("]")
                                                .append("[DataMappedFilePosition=").append(indexedBinaryFile.getDataMappedFilePosition()).append("]")
                                                .append("[IndexedMappedFilePosition=").append(indexedBinaryFile.getIndexedMappedFilePosition()).append("]")
                                        ;
                                        log.info(sb.toString());
                                    }

                                    currentCommandSequence = sequenceUtility.incrementSequence(sourceSequenceIndex);
                                    eventSequence = sequenceUtility.incrementSequence(eventSequenceIndex);
                                    sourceSequence++;
                                }
//                                indexedBinaryFile.force();


                                if(!commandByteBuffer.hasRemaining()) {
                                    commandByteBuffer.clear();
                                    commandPosition = commandByteBuffer.position();
                                    writableSelectionKey.interestOps(SelectionKey.OP_WRITE);
                                }
                            }
                        }
                    }
                    else if(selectionKey.isWritable()) {
                        long eventLimit = eventPosition;
                        eventPosition = eventByteBuffer.position();
                        eventByteBuffer.flip();
                        if(eventByteBuffer.hasRemaining()) {
//                            indexedBinaryFile.force();
                            eventBuffer.position((int)eventByteBuffer.position());
                            eventBuffer.limit((int)eventByteBuffer.limit());
                            int eventBytesSent = eventChannel.send(eventBuffer, eventGroup);
                            eventByteBuffer.position(eventBuffer.position());
                            eventByteBuffer.limit(eventBuffer.limit());
                            totalBytesSent += eventBytesSent;
//                            long eventSequence = sequenceUtility.incrementSequence(eventSequenceIndex);
                            long eventSequence = sequenceUtility.getSequence(eventSequenceIndex);
                            if((eventSequence <= 1000) || (eventSequence % logModCount == 0)) {
                                sb.setLength(0);
                                sb.append("event:")
                                        .append("[currentEventSequence=").append(currentEventSequence).append("]")
                                        .append("[nextEventSequence=").append(eventSequence).append("]")
                                        .append("[eventBytesSent=").append(eventBytesSent).append("]")
                                        .append("[totalBytesSent=").append(totalBytesSent).append("]")
                                        .append("[DataMappedFilePosition=").append(indexedBinaryFile.getDataMappedFilePosition()).append("]")
                                        .append("[IndexedMappedFilePosition=").append(indexedBinaryFile.getIndexedMappedFilePosition()).append("]")
                                ;
                                log.info(sb.toString());
                            }
                            if(!eventByteBuffer.hasRemaining()){
                              writableSelectionKey.interestOps(0);
                              eventByteBuffer.clear();
                              eventPosition = eventByteBuffer.position();
                            }
                        }
                    }

                    if (needsToSelectAgain) {
                        selectAgain(selector);
                        // Need to flip the optimized selectedKeys to get the right reference to the array
                        // and reset the index to -1 which will then set to 0 on the for loop
                        // to start over again.
                        //
                        // See https://github.com/netty/netty/issues/1523
                        selectionKeys = this.selectedKeys.flip();
                        i = -1;
                    }

                    final long ioTime = System.nanoTime() - ioStartTime;
                    final int ioRatio = this.ioRatio;
                }
//            }
        }
    }

    private void selectAgain(Selector selector) {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            log.warn("Failed to update SelectionKeys.", t);
        }
    }

    static void printDatagram(SocketAddress sa, ByteBuffer buf) {
        System.out.format("-- datagram from %s --\n",
                ((InetSocketAddress) sa).getAddress().getHostAddress());
        System.out.println(Charset.defaultCharset().decode(buf));
    }


    private static void printOptions(NetworkChannel channel, String prefix, String suffix) throws IOException {
        log.info(prefix + channel.getClass().getSimpleName() + suffix + " supports:");
        for (SocketOption<?> option : channel.supportedOptions()) {
            log.info("\t" + option.name() + ": " + channel.getOption(option));
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

    public static void main(String[] args) throws Exception {
        SequencerConfig sequencerConfig = new SequencerConfig();
        IndexedBinaryFileConfig indexedBinaryFileConfig = getIndexedBinaryFileConfig();
        sequencerConfig.setIndexedBinaryFileConfig(indexedBinaryFileConfig);
        Sequencer2 sequencer = new Sequencer2(sequencerConfig);
    }

    private static IndexedBinaryFileConfig getIndexedBinaryFileConfig() {
        FileSystem fileSystem = FileSystems.getDefault();
        Path directoryPath = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/");
        final String absoluteDirectoryPath = directoryPath.toFile().getAbsolutePath();
        String baseFileName = "sequencerIndexedBinaryFile";

        String indexFileSuffix = "index";
        String dataFileSuffix = "data";
        long dataFileBlockSize = ByteUnit.MEGABYTE.getBytes() * 256;
        long dataFileInitialFileSize = ((long)MappedFileBuffer.MAX_SEGMENT_SIZE)*50L;

        long indexFileBlockSize = ((long)BitUtil.SIZE_OF_LONG)*2L*1_000_000L; //accomodate 1,000,000 entries
        long indexFileInitialFileSize = ((long)BitUtil.SIZE_OF_LONG)*2L*50L*1_000_000L; //accomodate 1,000,000,000 entries
        boolean deleteIfExists = true;
        ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

        IndexedBinaryFileConfig config = new IndexedBinaryFileConfig(absoluteDirectoryPath, baseFileName, indexFileSuffix, dataFileSuffix,
//                Path directoryPathPath, Path dataFilePath, Path indexFilePath, File dataFile, File indexFile,
                byteOrder, dataFileBlockSize, dataFileInitialFileSize, indexFileBlockSize, indexFileInitialFileSize, deleteIfExists);
        return config;
    }
}
