package youngk.java.basic;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-11
 */
public class WordCountWithText {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile("/Users/youngk/flink/src/main/java/youngk/java/basic/data.txt");
        AggregateOperator<Tuple2<String, Integer>> flat = text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1)
                .setParallelism(1);
        flat.writeAsCsv("/Users/youngk/flink/src/main/java/youngk/java/basic/result", "\n", " ");
        env.execute("WordCountWithText");
    }


    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for(String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
