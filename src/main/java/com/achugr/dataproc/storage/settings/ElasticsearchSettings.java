package com.achugr.dataproc.storage.settings;

import java.io.Serializable;

public class ElasticsearchSettings implements Serializable {
    private String host;
    private int port;
    private String scheme;
    private String errorsIndex;
    private String dataIndex;

    public ElasticsearchSettings() {
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getErrorsIndex() {
        return errorsIndex;
    }

    public void setErrorsIndex(String errorsIndex) {
        this.errorsIndex = errorsIndex;
    }

    public String getDataIndex() {
        return dataIndex;
    }

    public void setDataIndex(String dataIndex) {
        this.dataIndex = dataIndex;
    }
}
