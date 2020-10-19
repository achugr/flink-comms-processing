package com.achugr.dataproc.storage;

import java.io.IOException;
import java.io.InputStream;

public interface ObjectStorage {
    void store(String id, InputStream is, long size) throws IOException;

    InputStream get(String id) throws IOException;

    /**
     * Might be very expensive, shouldn't be in public API
     *
     * @return count of items in storage
     */
    long countItems();
}
