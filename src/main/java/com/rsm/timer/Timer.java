package com.rsm.timer;

/**
 * Created by Raymond on 1/8/14.
 */
public interface Timer {

    int schedule(TimerHandler handler, long timeMillis);
    int schedule(TimerHandler handler, long timeMillis, long recurrenceInterval);

    void cancel(int timerHandle);
}
