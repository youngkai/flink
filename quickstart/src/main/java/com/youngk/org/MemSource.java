package com.youngk.org;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * FROM youngk
 * Created by youngk on 2019-07-01.
 */
public class MemSource implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            sourceContext.collect("flink spark storm");
        }
    }

    @Override
    public void cancel() {

    }
}
