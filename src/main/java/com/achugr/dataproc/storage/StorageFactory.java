package com.achugr.dataproc.storage;

import com.achugr.dataproc.storage.settings.ObjectStorageSettings;
import com.achugr.dataproc.storage.settings.StorageSettings;
import com.google.common.base.Suppliers;

import java.io.Serializable;
import java.util.function.Supplier;

public class StorageFactory implements Serializable {

    private final StorageSettings storageSettings;
    private transient Supplier<ObjectStorage> sourceStorageSupplier;
    private transient Supplier<ObjectStorage> archivesStorageSupplier;

    public StorageFactory(StorageSettings storageSettings) {
        this.storageSettings = storageSettings;
        init();
    }

    /**
     * Explicit init method call is required because of the default way how flink serializes messages (with kryo) -
     * constructor with arguments doesn't become called.
     */
    public void init() {
        this.sourceStorageSupplier = Suppliers.memoize(() -> initSourcesStorage(storageSettings.getSourceStorage()));
        this.archivesStorageSupplier = Suppliers.memoize(() -> initSourcesStorage(storageSettings.getArchivesStorage()));
    }

    public ObjectStorage getSourcesStorage() {
        return sourceStorageSupplier.get();
    }

    public ObjectStorage getArchivesStorage() {
        return archivesStorageSupplier.get();
    }

    private ObjectStorage initSourcesStorage(ObjectStorageSettings objectStorageSettings) {
        return new MinioStorage(objectStorageSettings.getEndpoint(),
                objectStorageSettings.getKey(),
                objectStorageSettings.getToken(),
                objectStorageSettings.getBucket());
    }
}
