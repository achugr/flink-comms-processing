package com.achugr.dataproc.storage.settings;

import java.io.Serializable;

public class LocalFsStorageSettings implements Serializable {
    private String path;

    public LocalFsStorageSettings() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
