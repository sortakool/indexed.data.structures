package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.clients.handlers.BinaryFileCommandEncoder;
import com.rsm.clients.handlers.BinaryFileCommandEncoder2;
import com.rsm.message.nasdaq.itch.v4_1.MoldUDP64Packet;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * @see com.rsm.buffer.ItchParser2
 *
 * Basic client that will send a basic message sleep and then send the message again
 */
public class Client2 {

    private static final Logger log = LogManager.getLogger(Client2.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup group;
    private InetSocketAddress groupAddress;

    private int port = 9999;
    private int counter = 0;
    private String mCastGroup = "FF02:0:0:0:0:0:0:3";
//    private String mCastGroup = "230.0.0.1";
//    private String name = "test";
    private DatagramChannel datagramChannel;

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

    final MoldUDP64Packet moldUDP64Packet = new MoldUDP64Packet();

    private final ByteBuf commandByteBuf = Unpooled.directBuffer(1024);
    private final ByteBuffer commandByteBuffer;
    private final DirectBuffer commandDirectBuffer;
    private final byte[] sessionBytes = "0123456789".getBytes();
    long commandPosition = 0;

    Path path;
    File file;
    MappedFileBuffer fileBuffer;
    MappedByteBuffer byteBuffer;
    DirectBuffer directBuffer;
    long filePosition = 0;

    public Client2() throws Exception {
        commandByteBuffer = commandByteBuf.nioBuffer(0, this.commandByteBuf.capacity());
        this.commandDirectBuffer = new DirectBuffer(commandByteBuffer);

        path = Paths.get(System.getProperty( "user.home" ) + "/Downloads/11092013.NASDAQ_ITCH41");
        file = path.toFile();
        fileBuffer = new MappedFileBuffer(file);
        filePosition = fileBuffer.position();
        long capacity = fileBuffer.capacity();
        byteBuffer = fileBuffer.buffer(filePosition);
        directBuffer = new DirectBuffer(byteBuffer);


        group = new NioEventLoopGroup();
        groupAddress = new InetSocketAddress(mCastGroup, port);

        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.IP_MULTICAST_IF, NetUtil.LOOPBACK_IF)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<DatagramChannel>() {

                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
//                        ch.pipeline().addLast(new LoggingHandler())
//                                .addLast(new MoldCommandOutgoingHandler())
//                                .addLast(new DatagramWrapperHandler(groupAddress))
//                        ;
                        ch.pipeline()
                                .addLast(new LoggingHandler())
//                                .addLast(new TimestampSecondsCommandEncoder(commandByteBuf, groupAddress));
                                .addLast(new BinaryFileCommandEncoder2(commandByteBuf, groupAddress));
                    }
                }).localAddress(port);

    }

    public void run() throws Exception {
        this.datagramChannel = (DatagramChannel) bootstrap.bind().sync().channel();

//        long seq = 0;
//        int tempLength;
        while(true) {
//            log.info("filePosition="+filePosition);

//            path = Paths.get(System.getProperty( "user.home" ) + "/Downloads/11092013.NASDAQ_ITCH41");
//            file = path.toFile();
//            fileBuffer = new MappedFileBuffer(file);
//            fileBuffer.setByteOrder(ByteOrder.BIG_ENDIAN);
//            filePosition = fileBuffer.filePosition();
//            long capacity = fileBuffer.capacity();
//            fileByteBuffer = fileBuffer.buffer(filePosition);
//            fileDirectBuffer = new DirectBuffer(fileByteBuffer);

//            short aShort = fileByteBuffer.getShort();

            commandByteBuf.clear();

            directBuffer.wrap(byteBuffer);
            int messageLength = directBuffer.getShort((int) filePosition, ByteOrder.BIG_ENDIAN);
            if(messageLength == 0) {
                break;
            }
//            seq++;
            long fileNextPosition = filePosition + 2 + messageLength;
            byteBuffer.position((int) filePosition);
            byteBuffer.limit((int)fileNextPosition);

            //create MoldUDP64 Packet
            moldUDP64Packet.wrapForEncode(commandDirectBuffer, (int) filePosition);
            // Downstream Packet Message Block
            moldUDP64Packet.downstreamPacketHeader()
                    .putSession(sessionBytes, (int) filePosition)
                    .sequenceNumber(counter)
                    .messageCount(1);//hard code to 1 for now

            commandByteBuf.getBytes((int)commandPosition, byteBuffer);

            datagramChannel.writeAndFlush(byteBuffer);
            datagramChannel.flush();
//            datagramChannel.pipeline().fireUserEventTriggered(fileByteBuffer);
//            datagramChannel.pipeline().writeAndFlush(fileByteBuffer);
//            datagramChannel.pipeline().fireUserEventTriggered(fileByteBuffer);

            filePosition += fileNextPosition;    //2 is for messageLength

        }
        log.info("finished");
    }

    private void stop() {
        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting client");
        Client2 client = new Client2();

        try {
            client.run();
        } finally {
            client.stop();
        }
    }

}