package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public interface BytesColumn extends Column<byte[]>  {

    byte[] DEFAULT_VALUE = new byte[]{};
}
