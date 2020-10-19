package com.achugr.dataproc.util;

import com.achugr.dataproc.storage.settings.ObjectStorageSettings;
import com.google.common.collect.Iterators;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class MinioUtils {
    private static final Logger log = LoggerFactory.getLogger(MinioUtils.class);
    public static final int MAX_KEYS = 1000;
    public static final int BATCH_SIZE = 100;

    public static void reinitBucket(ObjectStorageSettings settings) {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        try {
            MinioClient minioClient = MinioClient.builder()
                    .endpoint(settings.getEndpoint())
                    .credentials(settings.getKey(), settings.getToken())
                    .build();

            if (minioClient.bucketExists(BucketExistsArgs.builder().bucket(settings.getBucket()).build())) {
                List<Runnable> deletionTasks;
                do {
                    deletionTasks = getDeletionTasksBatch(settings, minioClient);
                    List<Future<?>> futures = deletionTasks.stream()
                            .map(executorService::submit)
                            .collect(Collectors.toList());
                    for (Future<?> future : futures) {
                        future.get();
                    }
                } while (!deletionTasks.isEmpty());
            } else {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(settings.getBucket()).build());
            }
        } catch (
                Exception e) {
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }

    }

    @Nonnull
    private static List<Runnable> getDeletionTasksBatch(ObjectStorageSettings settings, MinioClient minioClient) {
        List<Runnable> deletionTasks = new ArrayList<>();
        try {
            ListObjectsArgs listObjectsArgs = ListObjectsArgs.builder()
                    .recursive(true)
                    .maxKeys(MAX_KEYS)
                    .bucket(settings.getBucket())
                    .build();
            Iterable<Result<Item>> objectsIterator = minioClient.listObjects(listObjectsArgs);
            Iterators.partition(objectsIterator.iterator(), BATCH_SIZE).forEachRemaining(
                    batch -> {
                        Runnable deletionTask = () -> deleteBatch(minioClient, settings.getBucket(), batch);
                        deletionTasks.add(deletionTask);
                    }
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return deletionTasks;
    }

    private static void deleteBatch(MinioClient minioClient, String bucket, List<Result<Item>> batch) {
        log.info("Deleting batch of {} elements from {}", BATCH_SIZE, bucket);
        List<DeleteObject> deletes = batch.stream()
                .map(el -> {
                    try {
//                        by some reason base64 paddings '=' are replaced here with %3D
                        return new DeleteObject(el.get().objectName().replaceAll("%3D", "="));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucket).objects(deletes).build()).spliterator().forEachRemaining(
                deleteResult -> {
                    try {
                        deleteResult.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }
}
