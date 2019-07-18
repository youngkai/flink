package youngk.java.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-07-11
 */
public class CommonSource implements SourceFunction<Long> {

    private long count = 0L;

    private boolean isRunning = true;



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
