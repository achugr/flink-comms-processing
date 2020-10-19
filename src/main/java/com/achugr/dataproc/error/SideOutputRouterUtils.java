package com.achugr.dataproc.error;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.function.Predicate;

public class SideOutputRouterUtils {

    public static <T> DataStream<T> handleErrors(DataStream<T> stream, Predicate<T> filterPredicate, OutputTag<T> outputTag, SinkFunction<T> sinkFunction) {
        SingleOutputStreamOperator<T> filtered = stream.process(new SideOutputRouter<>(filterPredicate, outputTag)).returns(outputTag.getTypeInfo());
        filtered.getSideOutput(outputTag).addSink(sinkFunction);
        return filtered;
    }
}
