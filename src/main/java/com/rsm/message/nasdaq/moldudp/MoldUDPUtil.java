package com.rsm.message.nasdaq.moldudp;

/**
 * Created by rmanaloto on 3/22/14.
 */
public class MoldUDPUtil {
    /*
    from: http://www.nasdaqomx.com/digitalAssets/88/88319_itch-multicast-offering-inet-technical-1_9.pdf
    -When multiple messages are available for dissemination in the ITCH Multicast server, they
    are batched to be encapsulated in the same MoldUDP Downstream packet (which hence
    contains multiple message blocks; number as indicated in the Message Count field).
    - The maximum size of a MoldUDP Downstream packet is 1440 bytes. However, it may only
    contain complete messages (i.e. if a message block cannot fit in the MoldUDP packet, it is
    put in the next MoldUDP packet).
    - A MoldUDP Downstream packet will be subject to the following headers added by UDP and
    IP: 8 + 20, which in turn may give the max size of 1440 + 8 + 20 = 1468 bytes.
    - From the above, the following can be stated: if any hop along the route provides a MTU
    size lower than 1468 bytes, the MoldUDP packet will not arrive to its destination.
         */
    public static final int MAX_MOLDUDP_DOWNSTREAM_PACKET_SIZE = 1440;
}
