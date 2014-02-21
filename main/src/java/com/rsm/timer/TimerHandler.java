package com.rsm.timer;

/**
 * Created by Raymond on 1/8/14.
 */
public interface TimerHandler {

    void handle(long timestamp, long expectedTimestamp);

    TimerHandler NoOpTimerHandler = new TimerHandler() {
        @Override
        public void handle(long timestamp, long expectedTimestamp) {

        }
    };
}
