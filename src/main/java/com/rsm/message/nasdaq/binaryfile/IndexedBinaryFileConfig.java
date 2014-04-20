package com.rsm.message.nasdaq.binaryfile;

import java.io.File;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * Created by rmanaloto on 3/28/14.
 */
public class IndexedBinaryFileConfig {

    public static final String DEFAULT_DATA_FILE_SUFFIX = "dat";
    public static final String DEFAULT_INDEX_FILE_SUFFIX = "idx";

    private String directoryPath;
    private String baseFileName;
    private String indexFileSuffix = DEFAULT_INDEX_FILE_SUFFIX;
    private String dataFileSuffix = DEFAULT_DATA_FILE_SUFFIX;
//    private Path directoryPathPath;
//    private Path dataFilePath;
//    private Path indexFilePath;
//    private File dataFile;
//    private File indexFile;

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    private long dataFileBlockSize;
    private long dataFileInitialFileSize;
//    private long dataFileGrowBySize;

    private long indexFileBlockSize;
    private long indexFileInitialFileSize;
//    private long indexFileGrowBySize;

    private boolean deleteIfExists = false;

    public IndexedBinaryFileConfig() {
        super();
    }

    public IndexedBinaryFileConfig(String directoryPath, String baseFileName, String indexFileSuffix, String dataFileSuffix,
//                                   Path directoryPathPath, Path dataFilePath, Path indexFilePath, File dataFile, File indexFile,
                                   ByteOrder byteOrder, long dataFileBlockSize, long dataFileInitialFileSize, long indexFileBlockSize, long indexFileInitialFileSize, boolean deleteIfExists) {
        this.directoryPath = directoryPath;
        this.baseFileName = baseFileName;
        this.indexFileSuffix = indexFileSuffix;
        this.dataFileSuffix = dataFileSuffix;
//        this.directoryPathPath = directoryPathPath;
//        this.dataFilePath = dataFilePath;
//        this.indexFilePath = indexFilePath;
//        this.dataFile = dataFile;
//        this.indexFile = indexFile;
        this.byteOrder = byteOrder;
        this.dataFileBlockSize = dataFileBlockSize;
        this.dataFileInitialFileSize = dataFileInitialFileSize;
//        this.dataFileGrowBySize = dataFileGrowBySize;
        this.indexFileBlockSize = indexFileBlockSize;
        this.indexFileInitialFileSize = indexFileInitialFileSize;
//        this.indexFileGrowBySize = indexFileGrowBySize;
        this.deleteIfExists = deleteIfExists;
    }

    public String getDirectoryPath() {
        return directoryPath;
    }

    public void setDirectoryPath(String directoryPath) {
        this.directoryPath = directoryPath;
    }

    public String getBaseFileName() {
        return baseFileName;
    }

    public void setBaseFileName(String baseFileName) {
        this.baseFileName = baseFileName;
    }

    public String getIndexFileSuffix() {
        return indexFileSuffix;
    }

    public void setIndexFileSuffix(String indexFileSuffix) {
        this.indexFileSuffix = indexFileSuffix;
    }

    public String getDataFileSuffix() {
        return dataFileSuffix;
    }

    public void setDataFileSuffix(String dataFileSuffix) {
        this.dataFileSuffix = dataFileSuffix;
    }

//    public Path getDirectoryPathPath() {
//        return directoryPathPath;
//    }
//
//    public void setDirectoryPathPath(Path directoryPathPath) {
//        this.directoryPathPath = directoryPathPath;
//    }
//
//    public Path getDataFilePath() {
//        return dataFilePath;
//    }
//
//    public void setDataFilePath(Path dataFilePath) {
//        this.dataFilePath = dataFilePath;
//    }
//
//    public Path getIndexFilePath() {
//        return indexFilePath;
//    }
//
//    public void setIndexFilePath(Path indexFilePath) {
//        this.indexFilePath = indexFilePath;
//    }
//
//    public File getDataFile() {
//        return dataFile;
//    }
//
//    public void setDataFile(File dataFile) {
//        this.dataFile = dataFile;
//    }
//
//    public File getIndexFile() {
//        return indexFile;
//    }
//
//    public void setIndexFile(File indexFile) {
//        this.indexFile = indexFile;
//    }

    public ByteOrder getByteOrder() {
        return byteOrder;
    }

    public void setByteOrder(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    public long getDataFileBlockSize() {
        return dataFileBlockSize;
    }

    public void setDataFileBlockSize(long dataFileBlockSize) {
        this.dataFileBlockSize = dataFileBlockSize;
    }

    public long getDataFileInitialFileSize() {
        return dataFileInitialFileSize;
    }

    public void setDataFileInitialFileSize(long dataFileInitialFileSize) {
        this.dataFileInitialFileSize = dataFileInitialFileSize;
    }

//    public long getDataFileGrowBySize() {
//        return dataFileGrowBySize;
//    }
//
//    public void setDataFileGrowBySize(long dataFileGrowBySize) {
//        this.dataFileGrowBySize = dataFileGrowBySize;
//    }

    public long getIndexFileBlockSize() {
        return indexFileBlockSize;
    }

    public void setIndexFileBlockSize(long indexFileBlockSize) {
        this.indexFileBlockSize = indexFileBlockSize;
    }

    public long getIndexFileInitialFileSize() {
        return indexFileInitialFileSize;
    }

    public void setIndexFileInitialFileSize(long indexFileInitialFileSize) {
        this.indexFileInitialFileSize = indexFileInitialFileSize;
    }

//    public long getIndexFileGrowBySize() {
//        return indexFileGrowBySize;
//    }
//
//    public void setIndexFileGrowBySize(long indexFileGrowBySize) {
//        this.indexFileGrowBySize = indexFileGrowBySize;
//    }

    public boolean isDeleteIfExists() {
        return deleteIfExists;
    }

    public void setDeleteIfExists(boolean deleteIfExists) {
        this.deleteIfExists = deleteIfExists;
    }
}
