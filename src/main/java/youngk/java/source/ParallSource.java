package youngk.java.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-11
 */
public class ParallSource implements ParallelSourceFunction<Long> {

    private boolean isRunning = true;

    private long count = 0;


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            System.out.println("产生的数据是：" + count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
