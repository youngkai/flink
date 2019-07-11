package com.youngk.org;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * FROM youngk
 * Created by youngk on 2019-07-01.
 */
public class MemSink<T> implements SinkFunction<T> {
    /**
     * 没过来一条数据调用一次
     */
    @Override
    public void invoke(T value) throws Exception {
        System.out.println("MemSink:" + value);
    }

}

class RSink extends RichSinkFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(String value) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
