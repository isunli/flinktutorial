package com.example.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. read file
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        // 3. flatmap
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.
        KeyedStream<Tuple2<String, Long>, String> wordAndOneDataStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. sum
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneDataStream.sum(1);


        // 6. output
        sum.print();

        // 7 start
        env.execute();
    }
}
