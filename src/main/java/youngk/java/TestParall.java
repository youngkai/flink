package youngk.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import youngk.java.source.ParallSource;

/**
 * FROM youngk
 * Created by youngk on 2019-07-11.
 */
public class TestParall {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new ParallSource());
        SingleOutputStreamOperator<Long> out = source.map((MapFunction<Long, Long>) value -> value).setParallelism(3);
        out.print();
        env.execute("TestParall");
    }


}
