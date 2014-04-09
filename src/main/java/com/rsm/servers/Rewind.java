package com.rsm.servers;

import com.rsm.message.nasdaq.itch.v4_1.ReplayRequest;
import com.rsm.message.nasdaq.moldudp.MoldUDPUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Server that receives Mold Event and then adds sequences to the messages and forwards them
 */
public class Rewind {

    private static final Logger log = LogManager.getLogger(Rewind.class);

    public static final String EVENT_MULTICAST_IP = "FF02:0:0:0:0:0:0:4";
    public static final int EVENT_MULTICAST_PORT = 9001;

    public static final String TCP_REWIND_IP = "FF02:0:0:0:0:0:0:5";
    public static final int TCP_REWIND_PORT = 9002;

    private ServerSocketChannel replayChannel;
    private DatagramChannel eventChannel;
    int eventPosition = 0;

    ByteBuffer eventByteBuffer;
    DirectBuffer eventDirectBuffer;

    ByteBuffer replayByteBuffer;
    DirectBuffer replayDirectBuffer;

    MembershipKey eventMembershipKey;

    ReplayRequest replayRequest;

    public Rewind() throws Exception {
        eventByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        eventByteBuffer.order(ByteOrder.BIG_ENDIAN);
        eventDirectBuffer = new DirectBuffer(eventByteBuffer);

        replayByteBuffer = ByteBuffer.allocateDirect(MoldUDPUtil.MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE*2);
        replayByteBuffer.order(ByteOrder.BIG_ENDIAN);
        replayDirectBuffer = new DirectBuffer(replayByteBuffer);

        Selector selector = Selector.open();

        NetworkInterface networkInterface = getNetworkInterface();

        //connect to the event group
        eventChannel = DatagramChannel.open(StandardProtocolFamily.INET6);
        eventChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        InetSocketAddress eventInetSocketAddress = new InetSocketAddress(EVENT_MULTICAST_PORT);
        eventChannel.bind(eventInetSocketAddress);
        eventChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        InetAddress eventGroup = InetAddress.getByName(EVENT_MULTICAST_IP);
        eventMembershipKey = eventChannel.join(eventGroup, networkInterface);

        eventChannel.configureBlocking(false);
        final SelectionKey readableSelectionKey = eventChannel.register(selector, SelectionKey.OP_READ);


        //create TCP connection
        replayChannel = ServerSocketChannel.open();
        replayChannel.setOption(StandardSocketOptions.SO_REUSEADDR, false);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(TCP_REWIND_IP, TCP_REWIND_PORT);
        replayChannel.bind(null);
        replayChannel.configureBlocking(false);

        //we do not care about this until we have read in some events
        final SelectionKey replaySelectionKey = replayChannel.register(selector, 0);

        boolean active = true;
        while(active) {
            int updated = selector.selectNow();
            if (updated > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey selectionKey = iter.next();
                    iter.remove();

                    if (replaySelectionKey.isAcceptable()) {
                        //we have been connected to and need to read in the request from
                        //the client requesting a rewind

                        ServerSocketChannel ch = (ServerSocketChannel) readableSelectionKey.channel();

                        SocketChannel socketChannel = ch.accept();
                        if(socketChannel != null) {
                            socketChannel.read(replayByteBuffer);
                            replayByteBuffer.flip();
                            int replayPosition = replayByteBuffer.position();

                            replayRequest.wrapForDecode(replayDirectBuffer, replayPosition, ReplayRequest.BLOCK_LENGTH, ReplayRequest.SCHEMA_VERSION);

                            log.debug("Requesting " + replayRequest.messageCount() + "messages starting at " + replayRequest.sequenceNumber());

                        }

                    } else if (readableSelectionKey.isReadable()) {
                        //we got a message and should save it to our binary file
                        log.info("got something");
                        DatagramChannel ch = (DatagramChannel)selectionKey.channel();
                        SocketAddress readableSocketAddress = ch.receive(eventByteBuffer);

                        if (readableSocketAddress != null) {
                            eventByteBuffer.flip();

                            if(eventByteBuffer.hasRemaining()) {
                                //put the event in the file
                            }
                        }

                        //we are now ready to accept connections on tcp replay line
                        replaySelectionKey.interestOps(SelectionKey.OP_ACCEPT);
                    }
                }
            }
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

    public static void main(String[] args) throws Exception{
        Rewind rewind = new Rewind();
    }

}
