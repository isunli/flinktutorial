package com.example.char05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1","1", 10L),
                new Event("2", "2", 9L)
        );

        // map
        SingleOutputStreamOperator<Event> result = stream.filter(new MyFilter());

        // lambda
        SingleOutputStreamOperator<Event> result2 = stream.filter(event -> true);
        result.print("1");

    }

    // map function

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return true;
        }
    }
}
