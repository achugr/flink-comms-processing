package com.achugr.dataproc.storage.settings;

import java.io.Serializable;

public class SourceSettings implements Serializable {
    private LocalFsStorageSettings sourceDir;

    public LocalFsStorageSettings getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(LocalFsStorageSettings sourceDir) {
        this.sourceDir = sourceDir;
    }
}
