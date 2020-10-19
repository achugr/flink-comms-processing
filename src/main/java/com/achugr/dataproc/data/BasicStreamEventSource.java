package com.achugr.dataproc.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;

public class BasicStreamEventSource implements EventSource {

    private final String link;
    private final long size;
    private transient InputStream stream;

    public BasicStreamEventSource(@Nonnull String link, long size, @Nonnull InputStream stream) {
        this.link = link;
        this.size = size;
        this.stream = stream;
    }

    public BasicStreamEventSource(@Nonnull String link, long size) {
        this.link = link;
        this.size = size;
        this.stream = null;
    }

    @Override
    public boolean isByte() {
        return false;
    }

    @Nullable
    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Nullable
    @Override
    public String getLink() {
        return link;
    }

    @Nonnull
    @Override
    public InputStream getStream() {
        if (stream == null) {
            throw new RuntimeException("Stream source should be fetched before getting stream");
        }
        return stream;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BasicStreamEventSource that = (BasicStreamEventSource) o;

        return new EqualsBuilder()
                .append(link, that.link)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(link)
                .toHashCode();
    }
}
