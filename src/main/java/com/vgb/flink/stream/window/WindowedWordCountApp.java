package com.vgb.flink.stream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

;

public class WindowedWordCountApp {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        //listen to a stream of data coming in at localhost:9000 delimited by \n
        //make sure to execute the following first to ensure a socket is open first
        //nc -l localhost -p 9000
        final DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");


        final DataStream <Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        //1. split each line further by whitespace
                        //2. collect a tuple for each token
                        Stream.of(value.toLowerCase().split("\\s"))
                                .forEach(token -> out.collect(new Tuple2<>(token, 1)));
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // Print the results to the console. Note that here single-threaded printed is used, rather than multi-threading.
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}
