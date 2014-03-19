package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import javax.xml.crypto.Data;
import java.io.File;
import java.net.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;


/**
 * @see com.rsm.buffer.ItchParser2
 *
 * Basic client that will send a basic message sleep and then send the message again
 */
public class Client3 {

    private static final Logger log = LogManager.getLogger(Client3.class);

//    private final Bootstrap bootstrap;
//    private final EventLoopGroup group;
    private InetSocketAddress groupAddress;

    private int port = 9999;
    private int counter = 0;
    private String mCastGroup = "FF02:0:0:0:0:0:0:3";

    private final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();
    private final byte[] sessionBytes = "0123456789".getBytes();

//    private String mCastGroup = "230.0.0.1";
//    private String name = "test";
//    private DatagramChannel datagramChannel;

//    private final TimestampSecondsMessage timestampSecondsMessage = new TimestampSecondsMessage();
//    private final SystemEventMessage systemEventMessage = new SystemEventMessage();
//    private final StockDirectoryMessage stockDirectoryMessage = new StockDirectoryMessage();
//    private final StockTradingActionMessage stockTradingActionMessage = new StockTradingActionMessage();
//    private final RegSHORestrictionMessage regSHORestrictionMessage = new RegSHORestrictionMessage();
//    private final MarketParticipantPositionMessage marketParticipantPositionMessage = new MarketParticipantPositionMessage();
//    private final byte[] temp = new byte[1024];
//
//    //commands
//    private final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
//    private final SystemEventCommand systemEventCommand = new SystemEventCommand();
//    private final StockDirectoryCommand stockDirectoryCommand = new StockDirectoryCommand();
//    private final StockTradingActionCommand stockTradingActionCommand = new StockTradingActionCommand();
//    private final RegSHORestrictionCommand regSHORestrictionCommand = new RegSHORestrictionCommand();
//    private final MarketParticipantPositionCommand marketParticipantPositionCommand = new MarketParticipantPositionCommand();

    private final ByteBuf byteBuf = Unpooled.directBuffer(1024);

    Path path;
    File file;
    MappedFileBuffer fileBuffer;
    MappedByteBuffer fileByteBuffer;
    DirectBuffer fileDirectBuffer;
    long filePosition = 0;

    final ByteBuffer commandByteBuffer;
    final DirectBuffer commandDirectBuffer;
    long commandPosition;

    DatagramPacket datagramPacket;
    MulticastSocket ms;
    java.nio.channels.DatagramChannel datagramChannelClient;
    MembershipKey membershipKey = null;
    Selector selector;

    public Client3() throws Exception {
        path = Paths.get(System.getProperty( "user.home" ) + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
        fileBuffer = new MappedFileBuffer(file);
        filePosition = fileBuffer.position();
        long capacity = fileBuffer.capacity();
        fileByteBuffer = fileBuffer.buffer(filePosition);
        fileDirectBuffer = new DirectBuffer(fileByteBuffer);

//        commandByteBuffer = ByteBuffer.allocateDirect(2048);
        commandByteBuffer = ByteBuffer.allocate(2048);
        commandByteBuffer.order(ByteOrder.BIG_ENDIAN);
        commandDirectBuffer = new DirectBuffer(commandByteBuffer);
        commandPosition = 0;

        //taken from http://books.google.com/books?id=kELcexu0pAcC&pg=PA371&lpg=PA371&dq=java+multicast+bytebuffer&source=bl&ots=SyEWLrM71V&sig=kLUEnaQxv3zBIO7VuQv4cHAsrE8&hl=en&sa=X&ei=TeQjU6DsO6H7yAHv2IGgDQ&ved=0CGsQ6AEwBg#v=onepage&q=java%20multicast%20bytebuffer&f=false
        try {




//            InetAddress ia = InetAddress.getByName(mCastGroup);
//            byte[] data = "Here's some multicast data\r\n".getBytes();
////            int port = p;
//            dp = new DatagramPacket(data, data.length, ia, port);
//            ms = new MulticastSocket();
//
//            //Get a datagram channel object to act as a client
//
//
//            //Get the multicast group reference to send data to
//            InetSocketAddress group = new InetSocketAddress(mCastGroup, port);




            // Bind the client any available address

        }
        catch (Exception e) {
            log.info("error", e);
        }

//        group = new NioEventLoopGroup();
//        groupAddress = new InetSocketAddress(mCastGroup, port);
//
//        bootstrap = new Bootstrap();
//        bootstrap.group(group)
//                .channel(NioDatagramChannel.class)
//                .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
//                .option(ChannelOption.SO_REUSEADDR, true)
//                .handler(new ChannelInitializer<DatagramChannel>() {
//
//                    @Override
//                    protected void initChannel(DatagramChannel ch) throws Exception {
////                        ch.pipeline().addLast(new LoggingHandler())
////                                .addLast(new MoldCommandOutgoingHandler())
////                                .addLast(new DatagramWrapperHandler(groupAddress))
////                        ;
//                        ch.pipeline()
//                                .addLast(new LoggingHandler())
////                                .addLast(new TimestampSecondsCommandEncoder(byteBuf, groupAddress));
//                                .addLast(new BinaryFileCommandEncoder(byteBuf, groupAddress));
//                    }
//                }).localAddress(port);

    }

    InetSocketAddress commandInetSocketAddress = null;

    public void run() throws Exception {
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

        //create, configure and bind the client datagram channel
        datagramChannelClient = java.nio.channels.DatagramChannel.open(StandardProtocolFamily.INET6);
        commandInetSocketAddress = new InetSocketAddress(mCastGroup, port);
        datagramChannelClient.bind(commandInetSocketAddress);
        datagramChannelClient.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
        datagramChannelClient.configureBlocking(false);

//        MulticastSocket multicastSocket = new MulticastSocket();
        datagramPacket = new DatagramPacket(commandByteBuffer.array(), commandByteBuffer.arrayOffset(), commandByteBuffer.remaining());

        selector = Selector.open();

        datagramChannelClient.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);


//        this.datagramChannel = (DatagramChannel) bootstrap.bind().sync().channel();

//        long seq = 0;
//        int tempLength;
        boolean active = true;
        while(active) {
            //1. check timers

            int selected = selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectionKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                if(key.isAcceptable()) {

                }
                else if(key.isConnectable()) {

                }
                else if(key.isReadable()) {

                }
                else if(key.isWritable()) {
//                    fileDirectBuffer.wrap(fileByteBuffer);
                    short messageLength = fileDirectBuffer.getShort((int) filePosition, ByteOrder.BIG_ENDIAN);
                    if(messageLength == 0) {
                        active = false;
                    }
//            seq++;
                    long nextPosition = filePosition + 2 + messageLength;
                    fileByteBuffer.position((int) filePosition);
                    fileByteBuffer.limit((int) nextPosition);

                    //create MoldUDP64 Packet
                    long startingCommandPosition = commandPosition;
                    moldUDP64Packet.wrapForEncode(commandDirectBuffer, (int)commandPosition);
                    // Downstream Packet Message Block
                    moldUDP64Packet.downstreamPacketHeader()
                            .putSession(sessionBytes, (int) filePosition)
                            .sequenceNumber(counter)
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

//                    DatagramPacket datagramPacket = new DatagramPacket()

//                    int sentBytes = datagramChannelClient.send(commandByteBuffer, commandInetSocketAddress);
//                    int bytesWrittern = datagramChannelClient.write(commandByteBuffer);
//                    datagramChannelClient.
                    filePosition += nextPosition;    //2 is for messageLength
                }
            }
        }
        log.info("finished");
    }

    private void stop() {
//        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting client");
        Client3 client = new Client3();

        try {
            client.run();
        } finally {
            client.stop();
        }
    }

}