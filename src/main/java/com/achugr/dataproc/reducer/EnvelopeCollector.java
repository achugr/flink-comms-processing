package com.achugr.dataproc.reducer;

import com.achugr.dataproc.data.EnvelopeBatch;
import com.achugr.dataproc.data.EnvelopeContext;
import com.achugr.dataproc.data.EventEnvelope;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

public class EnvelopeCollector implements AggregateFunction<EventEnvelope, EnvelopeBatch, EnvelopeBatch> {
    @Override
    public EnvelopeBatch createAccumulator() {
        return new EnvelopeBatch(Lists.newArrayList(), new EnvelopeContext(new HashMap<>()));
    }

    @Override
    public EnvelopeBatch add(EventEnvelope value, EnvelopeBatch accumulator) {
        accumulator.getEvents().add(value);
        accumulator.getContext().getContext().putAll(value.getContext().getContext());
        return accumulator;
    }

    @Override
    public EnvelopeBatch getResult(EnvelopeBatch accumulator) {
        return accumulator;
    }

    @Override
    public EnvelopeBatch merge(EnvelopeBatch a, EnvelopeBatch b) {
        a.getEvents().addAll(b.getEvents());
        a.getContext().getContext().putAll(b.getContext().getContext());
        return a;
    }
}
