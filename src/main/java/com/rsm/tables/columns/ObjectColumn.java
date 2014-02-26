package com.rsm.tables.columns;

import java.io.Serializable;

/**
 * Created by Raymond on 12/18/13.
 */
public interface ObjectColumn extends Column<Serializable>  {

    Serializable DEFAULT_VALUE = new Serializable() {};

}
