package com.achugr.dataproc.sink.rolling;

import com.achugr.dataproc.data.EnvelopeBatch;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

/**
 * We set that archive should be rolled on every new event, because event is already a batch of events.
 */
public class RollOnNewBatchPolicy extends CheckpointRollingPolicy<EnvelopeBatch, String> {

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, EnvelopeBatch element) {
        return true;
    }

    @Override
    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState, long currentTime) {
        return false;
    }
}
