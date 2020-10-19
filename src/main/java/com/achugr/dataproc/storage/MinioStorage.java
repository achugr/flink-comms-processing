package com.achugr.dataproc.storage;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

public class MinioStorage implements ObjectStorage {

    private static final Logger log = LoggerFactory.getLogger(MinioStorage.class);

    public static final int PART_SIZE = 5 * 1024 * 1024;
    private final transient MinioClient minioClient;
    private final String bucket;

    public MinioStorage(String endpoint, String key, String secret, String bucket) {
        OkHttpClient httpClient = new OkHttpClient.Builder().build();
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(key, secret)
                .httpClient(httpClient)
                .build();
        this.bucket = bucket;
    }

    @Override
    public void store(String id, InputStream is, long size) throws IOException {
        PutObjectArgs put = PutObjectArgs.builder()
                .bucket(bucket)
                .object(id)
                .stream(is, size, PART_SIZE)
                .build();
        try {
            minioClient.putObject(put);
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream get(String id) throws IOException {
        try {
            GetObjectArgs get = GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(id)
                    .build();
            return minioClient.getObject(get);
        } catch (MinioException | NoSuchAlgorithmException | InvalidKeyException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long countItems() {
        ListObjectsArgs listObjectsArgs = ListObjectsArgs.builder().recursive(true).bucket(bucket).build();
        Iterator<Result<Item>> objectsIterator = minioClient.listObjects(listObjectsArgs).iterator();
        long count = 0;
        while (objectsIterator.hasNext()) {
            objectsIterator.next();
            count++;
        }

        return count;
    }
}
