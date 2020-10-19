package com.achugr.dataproc.data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.Serializable;

/**
 * This represents source of the event. It might be inline for small payloads
 * or reference to external storage for big payloads.
 */
public interface EventSource extends Serializable {
    boolean isByte();

    @Nullable
    byte[] getBytes();

    @Nullable
    String getLink();

    @Nonnull
    InputStream getStream();

    long getSize();
}
