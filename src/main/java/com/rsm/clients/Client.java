package com.rsm.clients;

import com.rsm.buffer.MappedFileBuffer;
import com.rsm.clients.handlers.TimestampSecondsCommandEncoder;
import com.rsm.message.nasdaq.itch.v4_1.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * @see com.rsm.buffer.ItchParser2
 *
 * Basic client that will send a basic message sleep and then send the message again
 */
public class Client {

    private static final Logger log = LogManager.getLogger(Client.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup group;
    private InetSocketAddress groupAddress;

    private int port = 9999;
    private int counter = 0;
    private String mCastGroup = "FF02:0:0:0:0:0:0:3";
//    private String mCastGroup = "230.0.0.1";
    private String name = "test";
    private DatagramChannel datagramChannel;

    private final TimestampSecondsMessage timestampSecondsMessage = new TimestampSecondsMessage();
    private final SystemEventMessage systemEventMessage = new SystemEventMessage();
    private final StockDirectoryMessage stockDirectoryMessage = new StockDirectoryMessage();
    private final StockTradingActionMessage stockTradingActionMessage = new StockTradingActionMessage();
    private final RegSHORestrictionMessage regSHORestrictionMessage = new RegSHORestrictionMessage();
    private final MarketParticipantPositionMessage marketParticipantPositionMessage = new MarketParticipantPositionMessage();
    private final byte[] temp = new byte[1024];

    //commands
    private final TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
    private final SystemEventCommand systemEventCommand = new SystemEventCommand();
    private final StockDirectoryCommand stockDirectoryCommand = new StockDirectoryCommand();
    private final StockTradingActionCommand stockTradingActionCommand = new StockTradingActionCommand();
    private final RegSHORestrictionCommand regSHORestrictionCommand = new RegSHORestrictionCommand();
    private final MarketParticipantPositionCommand marketParticipantPositionCommand = new MarketParticipantPositionCommand();

    private final ByteBuf byteBuf = Unpooled.directBuffer(1024);

    public Client() {
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
                                .addLast(new TimestampSecondsCommandEncoder(byteBuf, groupAddress));
                    }
                }).localAddress(port);

    }

    public void run() throws Exception {
        this.datagramChannel = (DatagramChannel) bootstrap.bind().sync().channel();


        //we do not need to join the multicast unless we want to get messages
        //ch.joinGroup(groupAddress, NetUtil.LOOPBACK_IF).sync();


        Path path = Paths.get(System.getProperty( "user.home" ) +
                "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        MappedFileBuffer fileBuffer = new MappedFileBuffer(file);

        long position = fileBuffer.position();

        MappedByteBuffer byteBuffer = fileBuffer.buffer(position);
        final DirectBuffer directBuffer = new DirectBuffer(byteBuffer);

        ByteBuf commandByteBuf = Unpooled.directBuffer(1024);
        ByteBuffer commandBuffer = commandByteBuf.nioBuffer();
        DirectBuffer commandDirectBuffer = new DirectBuffer(commandBuffer);

        long seq = 0;
        int tempLength;
        while(true) {
//            log.info("filePosition="+filePosition);

            int messageLength = directBuffer.getShort((int) position, ByteOrder.BIG_ENDIAN);
            if(messageLength == 0) {
                break;
            }
            seq++;

            position += 2;  //move filePosition past length

            byte messageType = directBuffer.getByte((int)position);
            ITCHMessageType itchMessageType = ITCHMessageType.get(messageType);

            ITCHMessageType retrievedItchMessageType;

            switch(itchMessageType) {
                case TIMESTAMP_SECONDS:
                    timestampSecondsMessage.wrapForDecode(directBuffer, (int)position, timestampSecondsMessage.sbeBlockLength(), timestampSecondsMessage.sbeSchemaVersion());
                    retrievedItchMessageType = timestampSecondsMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long seconds = timestampSecondsMessage.seconds();
//                    log.info("[seconds="+seconds+"]");

//                    datagramChannel.writeAndFlush(timestampSecondsMessage);
                    datagramChannel.write(timestampSecondsMessage);

                    break;
                case SYSTEM_EVENT:
                    systemEventMessage.wrapForDecode(directBuffer, (int)position, systemEventMessage.sbeBlockLength(), systemEventMessage.sbeSchemaVersion());
                    retrievedItchMessageType = systemEventMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long timestamp = systemEventMessage.timestamp();
                    byte eventCode = systemEventMessage.eventCode();
//                    log.info("[timestamp="+timestamp+"][eventCode="+eventCode+"]");
                    //TODO create command and send command over datagram channel
                    break;
                case STOCK_DIRECTORY:
                    stockDirectoryMessage.wrapForDecode(directBuffer, (int)position, stockDirectoryMessage.sbeBlockLength(), stockDirectoryMessage.sbeSchemaVersion());
                    retrievedItchMessageType = stockDirectoryMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    long nanoseconds = stockDirectoryMessage.nanoseconds();
                    tempLength = stockDirectoryMessage.getStock(temp, 0);
                    byte marketCategory = stockDirectoryMessage.marketCategory();
                    byte financialStatusIndicator = stockDirectoryMessage.financialStatusIndicator();
                    long roundLotSize = stockDirectoryMessage.roundLotSize();
                    YesNoType yesNo = stockDirectoryMessage.roundLotsOnly();
//                    log.info("[nanoseconds="+nanoseconds+"][stock="+new String(temp, 0, StockDirectoryMessage.stockLength())+"]");
                    //TODO create command and send command over datagram channel
                    break;
                case STOCK_TRADING_ACTION:
                    stockTradingActionMessage.wrapForDecode(directBuffer, (int)position, stockTradingActionMessage.sbeBlockLength(), stockTradingActionMessage.sbeSchemaVersion());
                    retrievedItchMessageType = stockTradingActionMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    tempLength = stockTradingActionMessage.getStock(temp, 0);
                    TradingStateType tradingState = stockTradingActionMessage.tradingState();
                    byte reserved = stockTradingActionMessage.reserved();
                    tempLength = stockTradingActionMessage.getReason(temp, 0);
                    break;
                case REG_SHO_RESTRICTION:
                    regSHORestrictionMessage.wrapForDecode(directBuffer, (int)position, regSHORestrictionMessage.sbeBlockLength(), regSHORestrictionMessage.sbeSchemaVersion());
                    retrievedItchMessageType = regSHORestrictionMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    tempLength = regSHORestrictionMessage.getStock(temp, 0);
                    RegSHOShortSalePriceTestRestriction regSHOShortSalePriceTestRestriction = regSHORestrictionMessage.regSHOAction();
                    break;
                case MARKET_PARTICIPANT_POSITION:
                    marketParticipantPositionMessage.wrapForDecode(directBuffer, (int)position, marketParticipantPositionMessage.sbeBlockLength(), marketParticipantPositionMessage.sbeSchemaVersion());
                    retrievedItchMessageType = marketParticipantPositionMessage.messageType();
                    assert(retrievedItchMessageType == itchMessageType);
                    tempLength = marketParticipantPositionMessage.getMpid(temp, 0);
                    tempLength = marketParticipantPositionMessage.getStock(temp, 0);
                    YesNoType primaryMarketMaker = marketParticipantPositionMessage.primaryMarketMaker();
                    MarketMakerMode marketMakerMode = marketParticipantPositionMessage.marketMakerMode();
                    MarketParticipantState marketParticipantState = marketParticipantPositionMessage.marketParticipantState();
                    break;
                case ADD_ORDER_NO_MPID_ATTRIBUTION:
                case ADD_ORDER_WITH_MPID_ATTRIBUTION:
                case ORDER_EXECUTED:
                case ORDER_EXECUTED_WITH_PRICE:
                case ORDER_CANCEL:
                case ORDER_DELETE:
                case ORDER_REPLACE:
                case TRADE_MESSAGE_NON_CROSS:
                case CROSS_TRADE:
                case BROKEN_TRADE:
                case NOII:
                case RETAIL_INTEREST:
                    break;
                default:
                    log.error("Unhandled type: " + (char)messageType);
                    break;
            }

            position += messageLength;

        }
        log.info("finished");
    }

    private void stop() {
        group.shutdownGracefully();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting client");
        Client client = new Client();

        try {
            client.run();
        } finally {
            client.stop();
        }
    }

}