package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class ByteEventSource implements EventSource {

    private final byte[] bytes;

    public ByteEventSource(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean isByte() {
        return true;
    }

    @Nullable
    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Nullable
    @Override
    public String getLink() {
        return null;
    }

    @Nonnull
    @Override
    public InputStream getStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public long getSize() {
        return bytes.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ByteEventSource that = (ByteEventSource) o;

        return new EqualsBuilder()
                .append(bytes, that.bytes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(bytes)
                .toHashCode();
    }
}
