package com.achugr.dataproc.error;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.function.Predicate;

public class SideOutputRouter<T> extends ProcessFunction<T, T> {
    private final Predicate<T> filterPredicate;
    private final OutputTag<T> outputTag;

    public SideOutputRouter(Predicate<T> filterPredicate, OutputTag<T> outputTag) {
        this.filterPredicate = filterPredicate;
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        if (filterPredicate.test(value)) {
            out.collect(value);
        } else {
            ctx.output(outputTag, value);
        }
    }
}
