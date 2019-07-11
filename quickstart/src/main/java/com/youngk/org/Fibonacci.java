package com.youngk.org;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.Random;

/**
 * FROM youngk
 * Created by youngk on 2019-07-03.
 */
public class Fibonacci {

    public static void main(String[] args) {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());


        //iterate API的参数5000，不是指迭代5000次，而是等待反馈输入的最大时间间隔为5秒。流被认为是无界的，所以无法像批处理迭代那样指定最大迭代次数。但它允许指定一个最大等待间隔，如果在给定的时间间隔里没有元素到来，那么将会终止迭代
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterativeStream = inputStream.map(new TupleTransformMapFunction()).iterate(5000);

        //这里的fibonacciStream只是一个代称，其中的数据并不是真正的斐波那契数列，其实就是上面那个五元组
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> fibonacciStream = iterativeStream.map(new FibonacciCalcStepFunction());

    }

    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private Random random = new Random();
        private volatile boolean isRunning = true;
        private int counter = 0;
        private final static int MAX_RANDOM_VALUE = 100;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && counter < MAX_RANDOM_VALUE) {
                int first = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;
                int second = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;

                if (first > second) {
                    continue;
                }

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class TupleTransformMapFunction extends RichMapFunction<Tuple2<Integer,
            Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
                Tuple2<Integer, Integer> inputTuples) {
            return new Tuple5<>(
                    inputTuples.f0,
                    inputTuples.f1,
                    inputTuples.f0,
                    inputTuples.f1,
                    0);
        }
    }

    private static class FibonacciCalcStepFunction extends RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
                Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) {
            return new Tuple5<>(
                    inputTuple.f0,
                    inputTuple.f1,
                    inputTuple.f3,
                    inputTuple.f2 + inputTuple.f3,
                    ++inputTuple.f4);
        }
    }
}
