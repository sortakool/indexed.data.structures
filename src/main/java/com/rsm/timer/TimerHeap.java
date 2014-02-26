package com.rsm.timer;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * min indexed priority queue implementation of {@link com.rsm.timer.Timer}
 *
 * {@see http://algs4.cs.princeton.edu/24pq/IndexMinPQ.java.html}
 *
 * Created by Raymond on 1/8/14.
 */
public class TimerHeap implements Timer {

    private static final long NOT_RECURRING_INTERVAL = -1;
    private static final long EMPTY_KEY = -2;

    private int NMAX;        // maximum number of elements on PQ
    private int N;           // number of elements on PQ
    private int[] pq;        // binary heap using 1-based indexing
    private int[] qp;        // inverse of pq - qp[pq[i]] = pq[qp[i]] = i
    private long[] keys;      // keys[i] = priority of i (timestamps)
    private long[] recurrenceIntervals;
    private TimerHandler[] timerHandlers;

    private int nextHandle = 0;

    /**
     * Initializes an empty indexed priority queue with indices between 0 and NMAX-1.
     *
     * @param NMAX the keys on the priority queue are index from 0 to NMAX-1
     * @throws java.lang.IllegalArgumentException if NMAX < 0
     */
    public TimerHeap(int NMAX) {
        if (NMAX < 0) throw new IllegalArgumentException();
        this.NMAX = NMAX;
        keys = new long[NMAX + 1];    // make this of length NMAX??
        pq   = new int[NMAX + 1];
        qp   = new int[NMAX + 1];                   // make this of length NMAX??
        for (int i = 0; i <= NMAX; i++) qp[i] = -1;

        Arrays.fill(keys, EMPTY_KEY);

        recurrenceIntervals = new long[NMAX + 1];
        Arrays.fill(recurrenceIntervals, NOT_RECURRING_INTERVAL);

        timerHandlers = new TimerHandler[NMAX + 1];
        Arrays.fill(timerHandlers, TimerHandler.NoOpTimerHandler);

        nextHandle = getNextHandle();
    }

    /**
     * Is the priority queue empty?
     * @return true if the priority queue is empty; false otherwise
     */
    public boolean isEmpty() {
        return N == 0;
    }

    /**
     * Is i an index on the priority queue?
     * @param i an index
     * @throws java.lang.IndexOutOfBoundsException unless (0 &le; i < NMAX)
     */
    public boolean contains(int i) {
        if (i < 0 || i >= NMAX) throw new IndexOutOfBoundsException();
        return qp[i] != -1;
    }

    /**
     * Returns the number of keys on the priority queue.
     * @return the number of keys on the priority queue
     */
    public int size() {
        return N;
    }

    /**
     * Associates key with index i.
     * @param i an index
     * @param key the key to associate with index i
     * @throws java.lang.IndexOutOfBoundsException unless 0 &le; i < NMAX
     * @throws IllegalArgumentException if there already is an item associated with index i
     */
    public void insert(int i, long key) {
        if (i < 0 || i >= NMAX) throw new IndexOutOfBoundsException();
        if (contains(i)) throw new IllegalArgumentException("index is already in the priority queue");
        N++;
        qp[i] = N;
        pq[N] = i;
        keys[i] = key;
        swim(N);
    }

    /**
     * Remove the key associated with index i.
     * @param i the index of the key to remove
     * @throws java.lang.IndexOutOfBoundsException unless 0 &le; i < NMAX
     * @throws java.util.NoSuchElementException no key is associated with index i
     */
    public void delete(int i) {
        if (i < 0 || i >= NMAX) throw new IndexOutOfBoundsException();
        if (!contains(i)) throw new NoSuchElementException("handle " + i + " is not in the priority queue");
        int index = qp[i];
        exch(index, N--);
        swim(index);
        sink(index);
        keys[i] = EMPTY_KEY;
        qp[i] = -1;
    }

    /**
     * Removes a minimum key and returns its associated index.
     * @return an index associated with a minimum key
     * @throws java.util.NoSuchElementException if priority queue is empty
     */
    public int delMin() {
        if (N == 0) throw new NoSuchElementException("Priority queue underflow");
        int min = pq[1];
        exch(1, N--);
        sink(1);
        qp[min] = -1;            // delete
        keys[pq[N+1]] = EMPTY_KEY;    // to help with garbage collection
        pq[N+1] = -1;            // not needed
        return min;
    }

    /**
     * Returns an index associated with a minimum key.
     * @return an index associated with a minimum key
     * @throws java.util.NoSuchElementException if priority queue is empty
     */
    public int minIndex() {
        if (N == 0) throw new NoSuchElementException("Priority queue underflow");
        return pq[1];
    }

    /**
     * Returns a minimum key.
     * @return a minimum key
     * @throws java.util.NoSuchElementException if priority queue is empty
     */
    public long minKey() {
        if (N == 0) throw new NoSuchElementException("Priority queue underflow");
        return keys[pq[1]];
    }

    /**
     * Returns the key associated with index i.
     * @param i the index of the key to return
     * @return the key associated with index i
     * @throws java.lang.IndexOutOfBoundsException unless 0 &le; i < NMAX
     * @throws java.util.NoSuchElementException no key is associated with index i
     */
    public long keyOf(int i) {
        if (i < 0 || i >= NMAX) throw new IndexOutOfBoundsException();
        if (!contains(i)) throw new NoSuchElementException("index is not in the priority queue");
        else return keys[i];
    }


    /**************************************************************
     * General helper functions
     **************************************************************/
    private boolean greater(int i, int j) {
        return keys[pq[i]] > (keys[pq[j]]);
    }

    private void exch(int i, int j) {
        int swap = pq[i];
        pq[i] = pq[j];
        pq[j] = swap;
        qp[pq[i]] = i;
        qp[pq[j]] = j;
    }


    /**************************************************************
     * Heap helper functions
     **************************************************************/
    private void swim(int k)  {
        while (k > 1 && greater(k/2, k)) {
            exch(k, k/2);
            k = k/2;
        }
    }

    private void sink(int k) {
        while (2*k <= N) {
            int j = 2*k;
            if (j < N && greater(j, j+1)) j++;
            if (!greater(k, j)) break;
            exch(k, j);
            k = j;
        }
    }

    /**
     * @return
     */
    protected int getNextHandle() {
        while(contains(nextHandle)) {
            nextHandle++;
        }
        return -1;
    }

    @Override
    public int schedule(TimerHandler handler, long timestampMicros) {
        return schedule(handler, timestampMicros, NOT_RECURRING_INTERVAL);
    }

    @Override
    public int schedule(TimerHandler handler, long timestampMicros, long recurrenceInterval) {
        if((recurrenceInterval != NOT_RECURRING_INTERVAL) && (recurrenceInterval <= 0)) {
            throw new IllegalArgumentException("recurrenceInterval should be greater than 0. But is " + recurrenceInterval);
        }
        int handle = getNextHandle();
        insert(handle, timestampMicros);
        recurrenceIntervals[handle] = recurrenceInterval;
        timerHandlers[handle] = handler;
        return handle;
    }

    @Override
    public void cancel(int timerHandle) {
        delete(timerHandle);
        recurrenceIntervals[timerHandle] = NOT_RECURRING_INTERVAL;
        timerHandlers[timerHandle] = TimerHandler.NoOpTimerHandler;
    }

    public void fireTimers() {
        long timeMillis = getTimestampMicros();
        long timerHandlerTimestamp;
        while(timeMillis<=(timerHandlerTimestamp=minKey())) {
            int timerHandle = delMin();
            long recurrenceInterval = recurrenceIntervals[timerHandle];
            TimerHandler timerHandler = timerHandlers[timerHandle];
            timerHandler.handle(timeMillis, timerHandlerTimestamp);
            if(recurrenceInterval != NOT_RECURRING_INTERVAL) {
                schedule(timerHandler, timeMillis+recurrenceInterval, recurrenceInterval);
            }
        }
    }

    public long getTimestampMicros() {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    }
}