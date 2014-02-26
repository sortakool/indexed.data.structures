package com.rsm.event;

/**
 * Created by rmanaloto on 2/25/14.
 */
public interface Event {

    long getSequence();
    int getSource();
    int getId();
    int getReference();
}
