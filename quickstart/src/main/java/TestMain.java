import com.youngk.org.RandomWordSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-06-27
 */
public class TestMain {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(new RandomWordSource());

        dataStreamSource.map(new UpperCaseMapFunc()).print();

        env.execute("sourceFunctionDemo");
    }

    public static final class UpperCaseMapFunc implements MapFunction<String, String> {

        @Override
        public String map(String s) {
            return s.toUpperCase();
        }
    }
}
