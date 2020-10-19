package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * We aggregate errors during the processing and persist them once message reaches final destination
 */
public class ProcError {

    private final String error;
    private final Type type;

    public ProcError(String error, Type type) {
        this.error = error;
        this.type = type;
    }

    public String getError() {
        return error;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        NON_TERMINAL,
        TERMINAL
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ProcError procError = (ProcError) o;

        return new EqualsBuilder()
                .append(error, procError.error)
                .append(type, procError.type)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(error)
                .append(type)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "ProcError{" +
                "error='" + error + '\'' +
                ", type=" + type +
                '}';
    }
}
