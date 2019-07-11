package youngk.java.basic;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

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
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws IOException {
            URL resource = this.getClass().getClassLoader().getResource("aa.txt");
            System.out.println(resource.getPath());
            File file = new File("src/main/resources/aa.txt");
            InputStream stream = new FileInputStream(file);
            int  n=0;
            StringBuffer sBuffer=new StringBuffer();
            while (n!=-1)
            {
                n=stream.read();
                char by=(char)n;
                sBuffer.append(by);
            }
            System.out.println(sBuffer.toString());
            String[] words = value.split(" ");
            for(String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
