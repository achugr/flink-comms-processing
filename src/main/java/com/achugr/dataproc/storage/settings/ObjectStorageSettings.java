package com.achugr.dataproc.storage.settings;

import org.apache.http.client.utils.URIBuilder;

import java.io.Serializable;
import java.net.URISyntaxException;

public class ObjectStorageSettings implements Serializable {
    private String endpoint;
    private String key;
    private String token;
    private String bucket;
    private String basePath;

    public ObjectStorageSettings() {
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getS3Uri() {
        try {
            return new URIBuilder()
                    .setScheme("s3")
                    .setHost(bucket)
                    .setPath(basePath != null ? basePath : "")
                    .build()
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
