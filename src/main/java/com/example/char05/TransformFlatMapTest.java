package com.example.char05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(1);

            DataStreamSource<Event> stream = env.fromElements(
                    new Event("1", "1", 10L),
                    new Event("2", "2", 9L)
            );
            System.out.println("gsge");

            // map


            // class
            SingleOutputStreamOperator<String> result2 = stream.flatMap(new MyFlatMap());
            result2.print("1");

            // lambda
            SingleOutputStreamOperator<String> result3 = stream.flatMap((Event e, Collector<String> c) -> {
                c.collect(e.url);
            }).returns(new TypeHint<String>(){});
            result3.print("3");
            result2.print("1");
            env.execute();

        }
    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event o, Collector<String> collector) throws Exception {
            collector.collect(o.user);
            collector.collect(o.url);
            collector.collect(o.timestamp.toString());
        }
    }
}
