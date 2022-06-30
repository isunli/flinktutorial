package com.example.char05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1","1", 10L),
                new Event("2", "2", 9L),
                new Event("2", "3", 11L),
                new Event("3", "3", 12L),
                new Event("3", "3", 412L)
        );

        // Keyby
        stream.keyBy(new KeySelector<Event, String>(){
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("user").print("Max"); // POJO class
        // max for one column, other columns use first event

        stream.keyBy((KeySelector<Event, String>) event -> event.user)
                .maxBy("user")
                .print("MaxBy"); // POJO class
        // max for all columns
        env.execute();

    }
}
