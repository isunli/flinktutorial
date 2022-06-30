package com.example.char05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1","1", 10L),
                new Event("2", "2", 9L)
        );

        // map
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        
        // class
        SingleOutputStreamOperator<Object> result2 = stream.map(new MapFunction<Event, Object>() {
            @Override
            public Object map(Event event) throws Exception {
                return event.user;
            }
        });

        // lambda

        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);
        result2.print("2");
        result.print("1");
        result3.print("3");
        env.execute();
    }

    // map function
    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
