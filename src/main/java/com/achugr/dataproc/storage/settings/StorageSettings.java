package com.achugr.dataproc.storage.settings;

import java.io.Serializable;

public class StorageSettings implements Serializable {
    private ObjectStorageSettings sourceStorage;
    private ObjectStorageSettings archivesStorage;
    private ObjectStorageSettings checkpointsStorage;
    private ElasticsearchSettings elasticsearch;
    private long maxInMemoryPayloadSize;

    public ObjectStorageSettings getSourceStorage() {
        return sourceStorage;
    }

    public void setSourceStorage(ObjectStorageSettings sourceStorage) {
        this.sourceStorage = sourceStorage;
    }

    public ElasticsearchSettings getElasticsearch() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticsearchSettings elasticsearch) {
        this.elasticsearch = elasticsearch;
    }

    public ObjectStorageSettings getArchivesStorage() {
        return archivesStorage;
    }

    public void setArchivesStorage(ObjectStorageSettings archivesStorage) {
        this.archivesStorage = archivesStorage;
    }

    public ObjectStorageSettings getCheckpointsStorage() {
        return checkpointsStorage;
    }

    public void setCheckpointsStorage(ObjectStorageSettings checkpointsStorage) {
        this.checkpointsStorage = checkpointsStorage;
    }

    public long getMaxInMemoryPayloadSize() {
        return maxInMemoryPayloadSize;
    }

    public void setMaxInMemoryPayloadSize(long maxInMemoryPayloadSize) {
        this.maxInMemoryPayloadSize = maxInMemoryPayloadSize;
    }
}
