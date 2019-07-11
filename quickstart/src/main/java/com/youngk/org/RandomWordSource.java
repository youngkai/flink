package com.youngk.org;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

/**
 * FROM youngk
 *
 * @author youngk
 * @date 2019-06-27
 */
public class RandomWordSource implements SourceFunction<String> {

    private static Logger logger = Logger.getLogger(RandomWordSource.class.getClass());
    private volatile boolean isRunning = true;
    private static final String[] words = new String[]{"The", "brown", "fox", "quick", "jump", "sucky", "5dolla"};
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            Thread.sleep(300);
            int rnd = (int)(Math.random() * 10 %words.length);
            logger.info("emit word: " + words[rnd]);
            sourceContext.collect(words[rnd]);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
