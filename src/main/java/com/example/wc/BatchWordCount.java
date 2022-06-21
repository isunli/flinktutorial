package com.example.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 read from file
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3. tokenize
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 key by
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple
                .groupBy(0);

        // 5. sum
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 6. output
        sum.print();


    }
}
