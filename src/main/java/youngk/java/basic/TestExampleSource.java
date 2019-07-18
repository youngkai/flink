package youngk.java.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import youngk.java.source.CommonSource;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-11
 */
public class TestExampleSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new CommonSource());
        SingleOutputStreamOperator<Long> map = source.map((MapFunction<Long, Long>) value -> {
            System.out.println("接收到的数据是：" + value);
            return value;
        });
        map.print().setParallelism(1);
        env.execute("TestExampleSource");
    }


}
