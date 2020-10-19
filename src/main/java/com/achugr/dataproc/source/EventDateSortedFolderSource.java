package com.achugr.dataproc.source;

import com.achugr.CommsEtlJob;
import com.achugr.dataproc.data.BasicStreamEventSource;
import com.achugr.dataproc.data.ByteEventSource;
import com.achugr.dataproc.data.EventEnvelope;
import com.achugr.dataproc.data.EventSource;
import com.achugr.dataproc.data.ProcError;
import com.achugr.dataproc.parsing.EmailParsingService;
import com.achugr.dataproc.storage.ObjectStorage;
import com.achugr.dataproc.storage.StorageFactory;
import com.achugr.dataproc.storage.settings.SourceSettings;
import com.achugr.dataproc.storage.settings.StorageSettings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is the FS-based source for events. It assumes that content of the folder
 * doesn't change. For further window based processing we have to make sure events are sent to source
 * in sorted order, so we sort them before consuming.
 */
public class EventDateSortedFolderSource implements SourceFunction<EventEnvelope> {
    private static final Logger log = LoggerFactory.getLogger(EventDateSortedFolderSource.class);

    private final StorageFactory storageFactory;
    private final SourceSettings sourceSettings;
    private final StorageSettings storageSettings;

    public EventDateSortedFolderSource(StorageFactory storageFactory, SourceSettings sourceSettings, StorageSettings storageSettings) {
        this.storageSettings = storageSettings;
        this.storageFactory = storageFactory;
        this.sourceSettings = sourceSettings;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        this.storageFactory.init();
        ObjectStorage objectStorage = storageFactory.getSourcesStorage();
        EmailParsingService emailParsingService = new EmailParsingService();
        List<PathWithDate> pathsWithDate = listFilesSortedByEventDate(emailParsingService);

        pathsWithDate.stream()
                .sorted(Comparator.comparing(PathWithDate::getDate))
                .parallel()
                .forEach(pathWithDate -> {
                            File file = pathWithDate.path.toFile();
                            try (InputStream is = FileUtils.openInputStream(file)) {
                                EventEnvelope eventEnvelope = emailParsingService.parse(is, pathWithDate.path.toAbsolutePath().toString());
                                String id = new String(Base64.getEncoder().encode(eventEnvelope.getEventMetadata().getDigest().getBytes()));
                                EventEnvelope.Builder envelopeBuilder = EventEnvelope.Builder.of(eventEnvelope);
//                                usually storages are not well optimized for many small writes, so
//                                better to keep max in memory payload size reasonable
//                                technically source storage is a temporary one, so we don't care much about
//                                proper events grouping
//                                TODO compress small events into batches for source storage
                                if (file.length() > storageSettings.getMaxInMemoryPayloadSize()) {
                                    objectStorage.store(id, FileUtils.openInputStream(file), file.length());
                                    EventSource eventSource = new BasicStreamEventSource(id, pathWithDate.getPath().toFile().length());
                                    envelopeBuilder.withEventSource(eventSource);
                                } else {
                                    EventSource eventSource = new ByteEventSource(IOUtils.toByteArray(FileUtils.openInputStream(file)));
                                    envelopeBuilder.withEventSource(eventSource);
                                }
                                ctx.collectWithTimestamp(envelopeBuilder.build(),
                                        eventEnvelope.getEventMetadata().getEventTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                            } catch (IOException e) {
                                log.error(e.getMessage(), e);
                                EventEnvelope failedEnvelope = EventEnvelope.Builder.envelop()
                                        .withError(new ProcError(e.getMessage(), ProcError.Type.TERMINAL))
                                        .build();
                                ctx.collectWithTimestamp(failedEnvelope, LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                            }
                        }
                );

        log.info("All data are sent");
//        artificial await for processing completion
        awaitArchivingCompletion();
    }

    /**
     * This is an artificial await for events source to be shutdown. It is needed because sinks supporting guaranteed delivery
     * works with the checkpoints and we have to await for all checkpoints to be committed before stopping source.
     * It's a trade-off required for proper finite-stream handling (i.e. batch data source).
     */
    private void awaitArchivingCompletion() {
        ObjectStorage archivesStorage = storageFactory.getArchivesStorage();
        long countOnPrevStep = -1;
        while (true) {
            try {
                long currentCount = archivesStorage.countItems();
                if (currentCount == countOnPrevStep && countOnPrevStep != -1) {
                    log.info("Archives count is: {} and doesn't growth", currentCount);
                    return;
                }
                countOnPrevStep = currentCount;
                log.info("Waiting for archives count ({}) to reach final count...", currentCount);
                TimeUnit.MILLISECONDS.sleep(CommsEtlJob.CHECKPOINT_INTERVAL);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Nonnull
    private List<PathWithDate> listFilesSortedByEventDate(EmailParsingService emailParsingService) throws IOException {
        List<PathWithDate> pathsWithDate = Collections.synchronizedList(new ArrayList<>());

        Files.walk(Paths.get(sourceSettings.getSourceDir().getPath())).parallel()
                .filter(Files::isRegularFile)
                .forEach(path -> {
                    try {
                        try (InputStream is = FileUtils.openInputStream(path.toFile())) {
                            EventEnvelope eventEnvelope = emailParsingService.parse(is, path.toAbsolutePath().toString());
                            pathsWithDate.add(new PathWithDate(path, eventEnvelope.getEventMetadata().getEventTime()));
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        pathsWithDate.add(new PathWithDate(path, LocalDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneId.systemDefault())));
                    }
                });
        return pathsWithDate;
    }

    @Override
    public void cancel() {
    }

    static class PathWithDate {
        private final Path path;
        private final LocalDateTime date;

        public PathWithDate(Path path, LocalDateTime date) {
            this.path = path;
            this.date = date;
        }

        public Path getPath() {
            return path;
        }

        public LocalDateTime getDate() {
            return date;
        }
    }
}
