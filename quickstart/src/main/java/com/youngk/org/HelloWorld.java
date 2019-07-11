package com.youngk.org;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-01
 */
public class HelloWorld {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.readTextFile("/data/flink/quickstart/hello");
        DataStream<String> dataStream;
        dataStream = env.addSource(new MemSource());
        dataStream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (input, collector) -> { String[] objs = input.split(" ");for (String obj : objs) { collector.collect(new Tuple2<>(obj, 1)); }}).keyBy(0).sum(1).addSink(new MemSink());
        env.execute();
    }


}
