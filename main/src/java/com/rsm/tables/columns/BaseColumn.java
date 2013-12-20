package com.rsm.tables.columns;

/**
 * Created by Raymond on 12/18/13.
 */
public abstract class BaseColumn<DEFAULTVALUETYPE> implements Column<DEFAULTVALUETYPE> {

    protected final String name;
    protected final Type type;
    protected final int numberOfBytes;
    protected DEFAULTVALUETYPE defaultvalue;

    protected BaseColumn(String name, Type type, int numberOfBytes) {
        this.name = name;
        this.type = type;
        this.numberOfBytes = numberOfBytes;
    }

    protected BaseColumn(String name, Type type, int numberOfBytes, DEFAULTVALUETYPE defaultvalue) {
        this.name = name;
        this.type = type;
        this.numberOfBytes = numberOfBytes;
        this.defaultvalue = defaultvalue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public int getNumberOfBytes() {
        return numberOfBytes;
    }

    @Override
    public DEFAULTVALUETYPE getDefaultValue() {
        return defaultvalue;
    }

    @Override
    public void setDefaultValue(DEFAULTVALUETYPE defaultvalue) {
        this.defaultvalue = defaultvalue;
    }

    public void notifyValueSet(Column column) {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseColumn)) return false;

        BaseColumn that = (BaseColumn) o;

        if (numberOfBytes != that.numberOfBytes) return false;
        if (defaultvalue != null ? !defaultvalue.equals(that.defaultvalue) : that.defaultvalue != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + numberOfBytes;
        result = 31 * result + (defaultvalue != null ? defaultvalue.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", numberOfBytes=" + numberOfBytes +
                ", defaultvalue=" + defaultvalue +
                '}';
    }
}
