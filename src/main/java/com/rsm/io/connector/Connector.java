package com.rsm.io.connector;

import com.rsm.message.event.Event;

/**
 * Created by rmanaloto on 2/25/14.
 */
public interface Connector {

    void onEvent(Event event);
}
