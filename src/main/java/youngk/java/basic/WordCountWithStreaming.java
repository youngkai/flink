package youngk.java.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FROM youngk
 *
 *
 * nc -l 9888
 *
 * @author youngk
 * @date 2019-07-11
 */
public class WordCountWithStreaming {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);
        sum.print().setParallelism(1);
        env.execute("WordCountWithStreaming");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            System.out.println("接收到的数据是：" + value);
            String[] words = value.split(" ");
            for(String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
