package com.rsm.message;

/**
 * Created by rmanaloto on 2/26/14.
 */
public interface Message {

    long getTimestampMillis();

    long getMicros();

    byte getMajor();

    byte getMinor();

    long getSource();

    int getId();

    int getReference();
}
