package com.achugr.dataproc.sink.streaming;

import com.achugr.dataproc.data.EnvelopeBatch;
import com.achugr.dataproc.data.EventEnvelope;
import com.achugr.dataproc.storage.BasicSyncEventFetcher;
import com.achugr.dataproc.storage.StorageFactory;
import com.achugr.dataproc.storage.SyncEventFetcher;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Defines strategy how to write a batch into output stream of the sink.
 */
public class EnvelopeBatchZipBulkWriterFactory implements BulkWriter.Factory<EnvelopeBatch> {
    private final StorageFactory storageFactory;

    public EnvelopeBatchZipBulkWriterFactory(StorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    @Override
    public BulkWriter<EnvelopeBatch> create(FSDataOutputStream out) {
        this.storageFactory.init();
        ZipOutputStream zos = new ZipOutputStream(out);
        SyncEventFetcher syncEventFetcher = new BasicSyncEventFetcher(storageFactory.getSourcesStorage());
        return new BulkWriter<EnvelopeBatch>() {
            private final Map<String, Integer> nameToCounter = new HashMap<>();

            @Override
            public void addElement(EnvelopeBatch value) throws IOException {
                for (EventEnvelope envelope : value.getEvents()) {
                    ZipEntry zipEntry = new ZipEntry(genName(envelope));
                    zos.putNextEntry(zipEntry);
                    IOUtils.copy(syncEventFetcher.fetch(envelope.getEventSource()).getStream(), zos);
                    zos.closeEntry();
                }
            }

            private String genName(EventEnvelope bulkWritable) {
                String origName = Paths.get(bulkWritable.getEventMetadata().getOriginalName()).getFileName().toString();
                int counter = nameToCounter.getOrDefault(origName, 0);
                nameToCounter.put(origName, counter + 1);
                return counter > 0 ? origName + "(" + counter + ")" : origName;
            }

            @Override
            public void flush() throws IOException {
                zos.flush();
            }

            @Override
            public void finish() throws IOException {
                zos.flush();
                zos.finish();
            }
        };
    }
}
