package com.achugr.dataproc.storage;

import com.achugr.dataproc.data.EventSource;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Fetches event source if needed from the object storage
 */
public interface SyncEventFetcher {
    @Nonnull
    EventSource fetch(@Nonnull EventSource eventSource) throws IOException;
}
