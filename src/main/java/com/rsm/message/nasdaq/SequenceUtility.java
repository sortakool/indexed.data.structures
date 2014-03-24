package com.rsm.message.nasdaq;

/**
 * Created by rmanaloto on 3/23/14.
 */
public class SequenceUtility {

    private final int inititalCapacity;
    private int currentCapacity;
    private int currentIndex = 0;
    private long[] sequences;

    public SequenceUtility(int inititalCapacity) {
        this.inititalCapacity = inititalCapacity;
        this.currentCapacity = this.inititalCapacity;
        this.sequences = new long[currentCapacity];
    }

    public int register() {
        grow();
        this.sequences[currentIndex] = 1;
        return currentIndex++;
    }

    private void grow() {
        if(this.currentIndex == (this.sequences.length-1)) {
            //TODO handle Integer overflow (ie array length is > Integer.MAX_VAlUE)
            long[] tempSequences = new long[sequences.length*2];
            System.arraycopy(this.sequences, 0, tempSequences, 0, this.sequences.length);
        }
    }

    public long incrementSequence(int index) {
        return adjustSequence(index, 1);
    }

    public long adjustSequence(int index, long delta) {
        this.sequences[index] += delta;
        return this.sequences[index];
    }

    public long setSequence(int index, long sequence) {
        long previousSequence = this.sequences[index];
        this.sequences[index] = sequence;
        return previousSequence;
    }

    public long getSequence(int index) {
        return this.sequences[index];
    }

    public long difference(int index, long sequence) {
        return (sequence - this.sequences[index]);
    }

    public boolean equals(int index, long sequence) {
        return (0 == difference(index, sequence));
    }

}
