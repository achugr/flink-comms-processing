/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.achugr;

import com.achugr.dataproc.data.EnvelopeBatch;
import com.achugr.dataproc.data.EnvelopeContext;
import com.achugr.dataproc.data.EventEnvelope;
import com.achugr.dataproc.data.ProcError;
import com.achugr.dataproc.duplicate.DuplicationFilter;
import com.achugr.dataproc.error.EnvelopProcessablePredicate;
import com.achugr.dataproc.error.SideOutputRouterUtils;
import com.achugr.dataproc.reducer.EnvelopeCollector;
import com.achugr.dataproc.sink.bucketing.EnvelopeBatchDateAndParticipantBucketAssigner;
import com.achugr.dataproc.sink.elasticsearch.BasicIndexingFunction;
import com.achugr.dataproc.sink.rolling.RollOnNewBatchPolicy;
import com.achugr.dataproc.sink.streaming.EnvelopeBatchZipBulkWriterFactory;
import com.achugr.dataproc.source.EventDateSortedFolderSource;
import com.achugr.dataproc.splitter.ParticipantBasedSplitter;
import com.achugr.dataproc.storage.StorageFactory;
import com.achugr.dataproc.storage.settings.ElasticsearchSettings;
import com.achugr.dataproc.storage.settings.SourceSettings;
import com.achugr.dataproc.storage.settings.StorageSettings;
import com.achugr.dataproc.util.StorageUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CommsEtlJob {

    private static final Logger log = LoggerFactory.getLogger(CommsEtlJob.class);

    public static final int CHECKPOINT_INTERVAL = 30000;

    public static void main(String[] args) throws Exception {
        /*
          Configuring env
         */
        StorageSettings storageSettings = getStorageSettings();
        StorageUtils.reinitStorage(storageSettings);

        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(CHECKPOINT_INTERVAL, CheckpointingMode.AT_LEAST_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(8);
        env.setStateBackend((StateBackend) new FsStateBackend(storageSettings.getCheckpointsStorage().getS3Uri()));

        FileSystem.initialize(GlobalConfiguration.loadConfiguration("./conf"),
                PluginUtils.createPluginManagerFromRootFolder(conf));

        StorageFactory storageFactory = createStorageFactory(storageSettings);

        /*
          Defining data source
         */
        DataStream<EventEnvelope> source = env
                .addSource(new EventDateSortedFolderSource(storageFactory, getSourceSettings(), storageSettings));

        /*
          Filtering fatal errors and route them to special sink (here just log)
         */
        final OutputTag<EventEnvelope> fatalErrorOutputTag = new OutputTag<EventEnvelope>("fatal-error") {
        };

        DataStream<EventEnvelope> filteredSource = SideOutputRouterUtils.handleErrors(source, new EnvelopProcessablePredicate(),
                fatalErrorOutputTag, new SinkFunction<EventEnvelope>() {
                    @Override
                    public void invoke(EventEnvelope value, Context context) {
                        log.error("Fatal error occurred: {}", value.getErrors().stream().map(ProcError::toString).collect(Collectors.joining()));
                    }
                });

        /*
          Filtering duplicates, quite naive here and can't say it suits paradigm well,
          because normally keying by hash when it's not that many duplicates produces too many partitions.
          But as usual we could ask external storage in async way to filter events effectively
         */

        SingleOutputStreamOperator<EventEnvelope> rawEvents = filteredSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy((KeySelector<EventEnvelope, String>) value -> value.getEventMetadata().getDigest())
                .flatMap(new DuplicationFilter());

        /*
          Connecting elasticsearch sink
         */
        ElasticsearchSink<EventEnvelope> esSink = createEsSink(storageSettings, storageFactory);
        rawEvents.addSink(esSink);

        /*
          Flat map by participants
          Split by participants
          Window by weeks (event time, not processing here, because historical data)
          Aggregate
         */
        SingleOutputStreamOperator<EnvelopeBatch> splitByParticipant = rawEvents
                .flatMap(new ParticipantBasedSplitter())
                .keyBy(envelope -> envelope.getContext(EnvelopeContext.PARTICIPANT))
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new EnvelopeCollector());

        /*
          Sink to zip archives on FS
         */
        StreamingFileSink<EnvelopeBatch> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(storageSettings.getArchivesStorage().getS3Uri()),
                new EnvelopeBatchZipBulkWriterFactory(storageFactory))
                .withBucketAssigner(new EnvelopeBatchDateAndParticipantBucketAssigner())
                .withRollingPolicy(new RollOnNewBatchPolicy())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".zip")
                        .build())
                .build();

        splitByParticipant.addSink(streamingFileSink);
        env.execute("Enron processing");
    }

    private static ElasticsearchSink<EventEnvelope> createEsSink(StorageSettings storageSettings, StorageFactory storageFactory) {
        List<HttpHost> httpHosts = new ArrayList<>();
        ElasticsearchSettings esSettings = storageSettings.getElasticsearch();
        httpHosts.add(new HttpHost(esSettings.getHost(), esSettings.getPort(), esSettings.getScheme()));
        return new ElasticsearchSink.Builder<>(httpHosts, new BasicIndexingFunction(storageFactory, storageSettings.getElasticsearch())).build();
    }

    private static StorageFactory createStorageFactory(StorageSettings storageSettings) {
        return new StorageFactory(storageSettings);
    }

    private static StorageSettings getStorageSettings() {
        Config config = loadConfig();
        return ConfigBeanFactory.create(config.getConfig("storage-settings"), StorageSettings.class);
    }

    private static SourceSettings getSourceSettings() {
        Config config = loadConfig();
        return ConfigBeanFactory.create(config.getConfig("source-settings"), SourceSettings.class);
    }

    private static Config loadConfig() {
        return ConfigFactory.load();
    }
}
