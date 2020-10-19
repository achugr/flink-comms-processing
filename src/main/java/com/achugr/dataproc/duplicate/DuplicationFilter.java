package com.achugr.dataproc.duplicate;

import com.achugr.dataproc.data.EventEnvelope;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Duplication filter for event stream.
 * It checks the state for given keyed stream event, if state exists - means that event occurred before.
 * State can has ttl, so we could avoid memory leak (here we speak about state memory that can be backed up by heap or disk,
 * depending on the strategy).
 *
 * Alternative way is to use external storage and async function for duplicates checking.
 */
public class DuplicationFilter extends RichFlatMapFunction<EventEnvelope, EventEnvelope> {
    private static final ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class);

    private ValueState<Boolean> operatorState;

    @Override
    public void open(Configuration configuration) {
        initValue();
    }

    private void initValue() {
//        TODO move period to configuration
        StateTtlConfig ttlConfig = StateTtlConfig
//                TODO ideally this should be a dynamic setting because for batch and stream processing
//                time to keep duplicate state is different
                .newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        operatorState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(EventEnvelope value, Collector<EventEnvelope> out) throws Exception {
        if (operatorState == null) {
            initValue();
        }
        if (operatorState.value() == null || !operatorState.value()) {
            out.collect(value);
            operatorState.update(true);
        }
    }
}
