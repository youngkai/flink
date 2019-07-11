package com.youngk.org;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-06-26
 */
public class MySelfSourceTest01 {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (true) {
                    int nextInt = random.nextInt(100);
                    ctx.collect("random : " + nextInt);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = dataStreamSource.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            String[] sps = value.split(":");
            return new Tuple2<>(value, Integer.parseInt(sps[1].trim()));
        }).keyBy(0).timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<String> apply = window.apply((WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>) (tuple, window1, input, out) -> input.forEach(x -> {
            System.out.println("apply function -> " + x.f0);
            out.collect(x.f0);
        }));

        apply.print();

        try {
            env.execute("myself_source_test01");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
