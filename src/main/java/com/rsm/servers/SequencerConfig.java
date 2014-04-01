package com.rsm.servers;

import com.rsm.message.nasdaq.binaryfile.IndexedBinaryFileConfig;

/**
 * Created by rmanaloto on 3/29/14.
 */
public class SequencerConfig {

    private IndexedBinaryFileConfig indexedBinaryFileConfig;

    public IndexedBinaryFileConfig getIndexedBinaryFileConfig() {
        return indexedBinaryFileConfig;
    }

    public void setIndexedBinaryFileConfig(IndexedBinaryFileConfig indexedBinaryFileConfig) {
        this.indexedBinaryFileConfig = indexedBinaryFileConfig;
    }
}
