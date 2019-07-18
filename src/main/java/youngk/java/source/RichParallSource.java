package youngk.java.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-15
 */
public class RichParallSource extends RichParallelSourceFunction<Long> {

    private boolean isRunning = true;

    private long count = 0L;


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
