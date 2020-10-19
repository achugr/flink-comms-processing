package com.achugr.dataproc.storage;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FsSimpleStorage implements ObjectStorage {

    private final String folder;

    public FsSimpleStorage(String folder) {
        this.folder = folder;
    }

    @Override
    public void store(String id, InputStream is, long size) throws IOException {
        FileUtils.copyInputStreamToFile(is, getFile(id));
    }

    private File getFile(String id) {
        return Paths.get(folder, id).toFile();
    }

    @Override
    public InputStream get(String id) throws IOException {
        return FileUtils.openInputStream(getFile(id));
    }

    @Override
    public long countItems() {
        try (Stream<Path> paths = Files.walk(Paths.get(folder)).parallel()) {
            return paths
                    .filter(Files::isRegularFile)
                    .count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
