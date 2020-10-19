package com.achugr.dataproc.sink.bucketing;

import com.achugr.dataproc.data.EnvelopeBatch;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class EnvelopeBatchDateAndParticipantBucketAssigner implements BucketAssigner<EnvelopeBatch, String> {

    private static final transient EnvelopeDateAndParticipantBucketAssigner bucketAssigner = new EnvelopeDateAndParticipantBucketAssigner();

    @Override
    public String getBucketId(EnvelopeBatch element, Context context) {
        return bucketAssigner.getBucketId(element.getEvents().get(0), context);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
