package com.youngk.org;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * FROM youngk
 * Created by youngk on 2019-07-01.
 */
public class RSource extends RichSourceFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
