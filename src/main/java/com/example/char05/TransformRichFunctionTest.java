package com.example.char05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1", "1", 10L),
                new Event("2", "2", 9L),
                new Event("2", "3", 11L),
                new Event("3", "3", 12L),
                new Event("3", "3", 412L)
        );

        stream.map(new MuRichMapFunction()).setParallelism(2).print();

        env.execute();
    }

    public static class MuRichMapFunction extends RichMapFunction<Event, Integer> {

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open: " + getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void close() throws Exception {
            super.close();

            System.out.println("close: " + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
