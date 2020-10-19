package com.achugr.dataproc.sink.elasticsearch;

import com.achugr.dataproc.data.EventEnvelope;
import com.achugr.dataproc.data.ProcError;
import com.achugr.dataproc.storage.StorageFactory;
import com.achugr.dataproc.storage.settings.ElasticsearchSettings;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function for indexing consumed messages.
 * Here we index event itself and errors collected during the processing.
 */
public class BasicIndexingFunction implements ElasticsearchSinkFunction<EventEnvelope> {
    private static final Logger log = LoggerFactory.getLogger(BasicIndexingFunction.class);

    private final StorageFactory storageFactory;
    private final ElasticsearchSettings elasticsearchSettings;

    public BasicIndexingFunction(StorageFactory storageFactory, ElasticsearchSettings elasticsearchSettings) {
        this.storageFactory = storageFactory;
        this.elasticsearchSettings = elasticsearchSettings;
    }

    @Override
    public void open() {
        storageFactory.init();
    }

    @Override
    public void process(EventEnvelope event, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createEventIndexRequest(event));
        createErrorsIndexRequest(event).forEach(indexer::add);
    }

    @Nonnull
    public List<IndexRequest> createErrorsIndexRequest(EventEnvelope envelope) {
        List<IndexRequest> errorIndexRequests = new ArrayList<>();
        for (ProcError procError : envelope.getErrors()) {
            try {
                Map<String, String> json = new HashMap<>();
                json.put("hash", envelope.getEventMetadata().getDigest());
                json.put("error", procError.getError());
                json.put("type", procError.getType().toString());
                errorIndexRequests.add(Requests.indexRequest()
                        .index(elasticsearchSettings.getErrorsIndex())
                        .source(json));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                Map<String, String> json = new HashMap<>();
                json.put("hash", envelope.getEventMetadata().getDigest());
                json.put("error", e.getMessage());
                json.put("type", ProcError.Type.TERMINAL.toString());
                errorIndexRequests.add(Requests.indexRequest()
                        .index(elasticsearchSettings.getErrorsIndex())
                        .source(json));
            }
        }
        return errorIndexRequests;
    }

    @Nonnull
    public IndexRequest createEventIndexRequest(EventEnvelope envelope) {
        try {
            Map<String, Object> json = new HashMap<>();
            json.put("hash", envelope.getEventMetadata().getDigest());
            if (envelope.getEventMetadata() != null) {
                Map<String, Object> indexableFields = envelope.getEvent().getIndexableFields().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue));
                json.putAll(indexableFields);
            }
            return Requests.indexRequest()
                    .index(elasticsearchSettings.getDataIndex())
                    .source(json);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Map<String, String> json = new HashMap<>();
            json.put("hash", envelope.getEventMetadata().getDigest());
            json.put("error", e.getMessage());
            json.put("type", ProcError.Type.TERMINAL.toString());
            return Requests.indexRequest()
                    .index(elasticsearchSettings.getErrorsIndex())
                    .source(json);
        }
    }
}
