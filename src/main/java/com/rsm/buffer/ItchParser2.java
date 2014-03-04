package com.rsm.buffer;

import com.rsm.message.nasdaq.binaryfile.BinaryFile;
import com.rsm.message.nasdaq.binaryfile.index.BinaryFileIndex;
import com.rsm.message.nasdaq.binaryfile.index.SequencePositionMap;
import com.rsm.message.nasdaq.itch.v4_1.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.io.File;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by rmanaloto on 2/27/14.
 */
public class ItchParser2 {

    private static final Logger log = LogManager.getLogger(MappedFileBuffer.class);

    public ItchParser2() throws Exception {
        Path path = Paths.get("/Users/rmanaloto/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        MappedFileBuffer fileBuffer = new MappedFileBuffer(file);

        BinaryFile binaryFile = new BinaryFile();




        long position = fileBuffer.position();

        MappedByteBuffer byteBuffer = fileBuffer.buffer(position);
        final DirectBuffer directBuffer = new DirectBuffer(byteBuffer);

        //public BinaryFile wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
        binaryFile.wrapForDecode(directBuffer, (int)position, binaryFile.sbeBlockLength(), binaryFile.sbeSchemaVersion());

        final byte[] messageBuffer = new byte[1024];
        DirectBuffer messageDirectBuffer = new DirectBuffer(messageBuffer);

        final byte[] commandBuffer = new byte[1024];
        DirectBuffer commandDirectBuffer = new DirectBuffer(commandBuffer);

        final byte[] eventBuffer = new byte[1024];
        DirectBuffer eventDirectBuffer = new DirectBuffer(eventBuffer);

        long seq = 0;
        while(true) {
            log.info("position="+position);

            int bytesCopied = binaryFile.getMessage(messageBuffer, (int) position, messageBuffer.length);
            if(bytesCopied == 0) {
                break;
            }
            seq++;

            byte messageType = messageBuffer[0];

            switch(messageType) {
                case 'T':
                    TimestampSecondsMessage timestampSecondsMessage = new TimestampSecondsMessage();
                    timestampSecondsMessage.wrapForDecode(messageDirectBuffer, 0, timestampSecondsMessage.sbeBlockLength(), timestampSecondsMessage.sbeSchemaVersion());
                    TimestampSecondsType payload = timestampSecondsMessage.payload();



                    int messageTypeLength = payload.getMessageType(messageBuffer, 0, messageBuffer.length);
                    assert(messageTypeLength == 1);
                    long seconds = payload.seconds();
                    log.info("[seconds="+seconds+"]");

                    TimestampSecondsType timestampSecondsType = new TimestampSecondsType();
                    timestampSecondsType.wrap(messageDirectBuffer, 0, timestampSecondsMessage.sbeSchemaVersion());
                    long seconds1 = timestampSecondsType.seconds();
                    log.info("[seconds="+seconds1+"]");
                    assert (seconds == seconds1);

                    TimestampSecondsCommand timestampSecondsCommand = new TimestampSecondsCommand();
                    timestampSecondsCommand.wrapForEncode(commandDirectBuffer, 0);
                    StreamHeader streamHeader = timestampSecondsCommand.streamHeader();
                    streamHeader
                            .timestampNanos(System.nanoTime())
                            .major((byte)'S')
                            .minor(messageType)
                            .source(1L)//convert a 8-bit ascii to a long
                            .id(seq)//should be source specific id sequence
                            .ref(seq)
                    ;
                    TimestampSecondsType payload1 = timestampSecondsCommand.payload();
                    payload1.seconds(seconds);
                    int size = timestampSecondsCommand.size();
                    log.info("size="+size);

                    break;
                case 'S':
                    SystemEventMessage systemEventMessage = new SystemEventMessage();
                    //public SystemEventMessage wrapForDecode(final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
                    systemEventMessage.wrapForDecode(directBuffer, (int)position, systemEventMessage.sbeBlockLength(), systemEventMessage.sbeSchemaVersion());
                    SystemEventType systemEventType = systemEventMessage.payload();
                    int timestamp = systemEventType.timestamp();
                    byte eventCode = systemEventType.eventCode();
                    log.info("[timestamp="+timestamp+"][eventCode="+eventCode+"]");
                    break;
                default:
                    log.error("Unhandled type: " + (char)messageType);
                    break;
            }

            position += (2 + bytesCopied);

            fileBuffer.position(position);
        }
        log.info("finished");
    }

    private long parsePayload(MappedFileBuffer fileBuffer, long position, long messageLength) {
        long orignalPosition = position;
        long expectedPosition = position + (messageLength);
        byte packetType = fileBuffer.get();
        log.info("packetType=" + (char)packetType);
        position+=1;
        fileBuffer.position(position);
        long returnedPosition = -1L;
        switch (packetType) {
            case 'S':
                returnedPosition = handleSystemEventMessage(fileBuffer, position, messageLength);
                break;
            case 'T':
                returnedPosition = handleTimestampMessage(fileBuffer, position, messageLength);
                break;
            case 'R':
                returnedPosition = handleStockDirectoryMessage(fileBuffer, position, messageLength);
                break;
            case 'H':
                returnedPosition = handleStockTradingActionMessage(fileBuffer, position, messageLength);
                break;
            case 'Y':
                returnedPosition = handleRegSHOShortSalePriceTestRestrictedIndicatorMessage(fileBuffer, position, messageLength);
                break;
            case 'L':
                returnedPosition = handleMarketParticipantPositionMessage(fileBuffer, position, messageLength);
                break;
            default:
                log.error("Unhandled packetType: " + (char)packetType);
                break;
        }
        fileBuffer.position(returnedPosition);
        return returnedPosition;
    }

    /**
     * T:
     * name: TimeStamp Seconds
     * fields:
     * - MessageType
     * - Second
     *
     * @param fileBuffer
     * @param position
     * @param messageLength
     */
    private long handleTimestampMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        //Number of seconds since midnight.
        int seconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        log.info("seconds=" + seconds);
        return fileBuffer.position();
    }

    /*
      S:
        name: System Event Message
        fields:
        - MessageType
        - Timestamp
        - EventCode

SYSTEM EVENT MESSAGE
Name Offset Length Value Notes
Message
Type
0 1 “S” System Event Message.
Timestamp 1 4 Integer Nanoseconds portion of the timestamp.
Event Code 5 1 Alpha See System Event Codes below.

     */
    private long handleSystemEventMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        int nanoseconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        byte eventCode = fileBuffer.get();
        log.info("[nanoseconds=" + nanoseconds +"][eventCode="+(char)eventCode+"]");
        return fileBuffer.position();
    }

    /*
Message Type 0 1 “R” Stock Directory Message
Timestamp -
Nanoseconds
1 4 Integer Nanoseconds portion of the timestamp.
Stock 5 8 Alpha Denotes the security symbol for the issue in the
NASDAQ execution system. Refer to Appendix
B for stock symbol convention information.
Market
Category
13 1 Alpha Indicates Listing market or listing market tier for
the issue

“N” = New York Stock Exchange (NYSE)
“A” = NYSE Amex
“P” = NYSE Arca
“Q” = NASDAQ Global Select MarketSM

“G” = NASDAQ Global MarketSM

“S” = NASDAQ Capital Market®

“Z” = BATS BZX Exchange




     */

    final byte[] stockData = new byte[8];

    private long handleStockDirectoryMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        int nanoseconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        fileBuffer.getBytes(stockData, 0, 8);
        String stock = new String(stockData, 0, 8);
        byte marketCategory = fileBuffer.get();
        byte financialStatusIndicator = fileBuffer.get();
        int roundLotSize = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        byte roundLotsOnly = fileBuffer.get();
        log.info("[nanoseconds=" + nanoseconds +"][stock="+stock+"][marketCategory="+(char)marketCategory+"]" +
                "[financialStatusIndicator="+financialStatusIndicator+"][roundLotSize="+roundLotSize+"]" +
                "[roundLotsOnly="+roundLotsOnly+"]");
        return fileBuffer.position();
    }

    /*
Message
Type
0 1 “H” Stock Trading Action Message.
Timestamp -
Nanoseconds
1 4 Integer Nanoseconds portion of the timestamp.
Stock 5 8 Alpha Stock symbol right padded with spaces.
Trading State 13 1 Alpha Indicates the current trading state for the stock.
Allowable values:

“H” = Halted across all U.S. equity markets /
SROs
“P” = Paused across all U.S. equity markets /
SROs (NASDAQ-listed securities only)
“Q” = Quotation only period for cross-SRO halt
or pause
“T” = Trading on NASDAQ
Reserved 14 1 Alpha Reserved.
Reason 15 4 Alpha Trading Action reason.
     */

    final byte[] reasonData = new byte[4];

    private long handleStockTradingActionMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        int nanoseconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        fileBuffer.getBytes(stockData, 0, 8);
        String stock = new String(stockData, 0, 8);
        byte tradingState = fileBuffer.get();
        byte reserved = fileBuffer.get();
        fileBuffer.getBytes(reasonData, 0, 4);
        String reason = new String(reasonData, 0, 4);
        log.info("[nanoseconds=" + nanoseconds +"][stock="+stock+"][tradingState="+(char)tradingState+"]" +
                "[reserved="+reserved+"][reason="+reason+"]");
        return fileBuffer.position();
    }

    /*
Message
Type
0 1 “Y” Reg SHO Short Sale Price Test Restricted
Indicator
Timestamp -
Nanoseconds
1 4 Integer Nanoseconds portion of the timestamp.
Stock 5 8 Alpha Stock symbol right padded with spaces.
Reg SHO
Action
13 1 Alpha Denotes the Reg SHO Short Sale Price Test
Restriction status for the issue at the time of the
message dissemination. Allowable values are:

“0” = No price test in place

“1” = Reg SHO Short Sale Price Test Restriction
in effect due to an intra-day price drop in
security

“2” = Reg SHO Short Sale Price Test Restriction
remains in effect

     */

    private long handleRegSHOShortSalePriceTestRestrictedIndicatorMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        int nanoseconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        fileBuffer.getBytes(stockData, 0, 8);
        String stock = new String(stockData, 0, 8);
        byte regShowAction = fileBuffer.get();
        log.info("[nanoseconds=" + nanoseconds +"][stock="+stock+"][regShowAction="+(char)regShowAction+"]");
        return fileBuffer.position();
    }

    /*
Message
Type
0 1 “L” Market Participant Position message
Timestamp -
Nanoseconds
1 4 Integer Nanoseconds portion of the
timestamp.
MPID 5 4 Alphabetic Denotes the market participant
identifier for which the position
message is being generated
Stock 9 8 Alphanumeric Denotes the security symbol for which
the position is being generated
Primary
Market Maker
17 1 Alphanumeric Indicates if the market participant firm
qualifies as a Primary Market Maker in
accordance with NASDAQ
marketplace rules

“Y” = primary market maker
“N” = non-primary market maker
Market Maker
Mode
18 1 Alphanumeric Indicates the quoting participant’s
registration status in relation to SEC
Rules 101 and 104 of Regulation M

“N” = normal
“P” = passive
“S” = syndicate
“R” = pre-syndicate
“L” = penalty
Market
Participant
State
19 1 Alphanumeric Indicates the market participant’s
current registration status in the issue
     */

    final byte[] mpidData = new byte[4];

    private long handleMarketParticipantPositionMessage(MappedFileBuffer fileBuffer, long position, long messageLength) {
        int nanoseconds = fileBuffer.getInt(ByteOrder.BIG_ENDIAN);
        fileBuffer.getBytes(mpidData, 0, 4);
        String mpid = new String(mpidData, 0, 4);
        fileBuffer.getBytes(stockData, 0, 8);
        String stock = new String(stockData, 0, 8);

        byte marketMakerMode = fileBuffer.get();
        byte marketParticipantState = fileBuffer.get();
        log.info("[nanoseconds=" + nanoseconds +"][mpid="+mpid+"][stock="+stock+"][marketMakerMode="+marketMakerMode+"][marketParticipantState="+marketParticipantState+"]");
        return fileBuffer.position();
    }


    /*
    messages:
      T:
        name: TimeStamp Seconds
        fields:
        - MessageType
        - Second
      S:
        name: System Event Message
        fields:
        - MessageType
        - Timestamp
        - EventCode
      R:
        name: Stock Directory
        fields:
        - MessageType
        - Timestamp
        - Stock
        - MarketCatagory
        - FinancialStatusIndicator
        - RoundLotSize
        - RoundLotsOnly
      H:
        name: Stock Trading Action
        fields:
        - MessageType
        - Timestamp
        - Stock
        - TradingState
        - Reserved
        - Reason
      Y:
        name: Reg SHO Short Sale Price Test Restricted Indicator
        fields:
        - MessageType
        - Timestamp
        - Stock
        - RegSHOAction
      L:
        name: Market Participant Position
        fields:
        - MessageType
        - Timestamp
        - MPID
        - Stock
        - PrimaryMarketMaker
        - MarketMakerMode
        - MarketParticipantState
      A:
        name: Add Order (No MPID Attribution)
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - BuySellIndicator
        - Shares
        - Stock
        - Price
      F:
        name: Add Order with MPID Attribution
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - BuySellIndicator
        - Shares
        - Stock
        - Price
        - Attribution
      E:
        name: Order Executed
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - ExecutedShares
        - MatchNumber
      C:
        name: Order Executed with Price
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - ExecutedShares
        - MatchNumber
        - Printable
        - ExecutionPrice
      X:
        name: Order Cancel
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - CanceledShares
      D:
        name: Order Delete
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
      U:
        name: Order Replace
        fields:
        - MessageType
        - Timestamp
        - OriginalOrderReferenceNumber
        - NewOrderReferenceNumber
        - Shares
        - Price
      P:
        name: Trade Message (non-cross)
        fields:
        - MessageType
        - Timestamp
        - OrderReferenceNumber
        - BuySellIndicator
        - Shares
        - Stock
        - Price
        - MatchNumber
      Q:
        name: Cross Trade
        fields:
        - MessageType
        - Timestamp
        - CrossShares
        - Stock
        - CrossPrice
        - MatchNumber
        - CrossType
      B:
        name: Broken Trade
        fields:
        - MessageType
        - Timestamp
        - MatchNumber
      I:
        name: Net Order Imbalance Indicator (NOII)
        fields:
        - MessageType
        - Timestamp
        - PairedShares
        - Imbalance
        - ImbalanceDirection
        - Stock
        - FarPrice
        - NearPrice
        - CurrentReferencePrice
        - CrossType
        - PriceVariationIndicator
     */

    public static void main(String[] args) throws Exception {
        ItchParser2 paser = new ItchParser2();
    }
}
