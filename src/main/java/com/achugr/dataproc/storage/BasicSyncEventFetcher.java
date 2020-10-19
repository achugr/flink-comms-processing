package com.achugr.dataproc.storage;

import com.achugr.dataproc.data.BasicStreamEventSource;
import com.achugr.dataproc.data.EventSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class BasicSyncEventFetcher implements SyncEventFetcher {

    private final ObjectStorage objectStorage;

    public BasicSyncEventFetcher(ObjectStorage objectStorage) {
        this.objectStorage = objectStorage;
    }

    @Nonnull
    @Override
    public EventSource fetch(@Nonnull EventSource eventSource) throws IOException {
        if (eventSource.isByte()) {
            return eventSource;
        } else {
            Objects.requireNonNull(eventSource.getLink());
            InputStream is = objectStorage.get(eventSource.getLink());
            return new BasicStreamEventSource(eventSource.getLink(), eventSource.getSize(), is);
        }
    }
}
