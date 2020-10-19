package com.achugr.dataproc.util;

import com.achugr.CommsEtlJob;
import com.achugr.dataproc.storage.settings.ElasticsearchSettings;
import com.achugr.dataproc.storage.settings.StorageSettings;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;


public class StorageUtils {

    private static final Logger log = LoggerFactory.getLogger(StorageUtils.class);

    public static void reinitStorage(StorageSettings storageSettings) {
        MinioUtils.reinitBucket(storageSettings.getSourceStorage());
        MinioUtils.reinitBucket(storageSettings.getArchivesStorage());
        MinioUtils.reinitBucket(storageSettings.getCheckpointsStorage());
        initMapping(storageSettings.getElasticsearch());
    }

    private static void initMapping(ElasticsearchSettings esSettings) {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(esSettings.getHost(), esSettings.getPort(), esSettings.getScheme())))) {
            reInitIndex(client, esSettings.getDataIndex(), "/elasticsearch/mapping/data.json");
            reInitIndex(client, esSettings.getErrorsIndex(), "/elasticsearch/mapping/errors.json");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static void reInitIndex(RestHighLevelClient client, String indexName, String mappingPath) throws IOException {
        if (client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
        }
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
            PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
            putMappingRequest.source(IOUtils.toString(CommsEtlJob.class.getResource(mappingPath), Charset.defaultCharset()),
                    XContentType.JSON);
            client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        }
    }
}
