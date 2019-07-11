package com.youngk.org;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * FROM youngk
 * Created by youngk on 2019-07-01.
 */
public class Hello {

    public static void main(String[] args) {

    }

    

    public class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private final static int MAX_RANDOM_VALUE = 100;
        private Random random  = new Random();
        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {
            while (isRunning && counter < MAX_RANDOM_VALUE) {
                int first = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;
                int second = random.nextInt(MAX_RANDOM_VALUE / 2 -1) + 1;
                if(first > second){
                    continue;
                }
                sourceContext.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

