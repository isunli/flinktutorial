package com.example.char06;

import com.example.char05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1", "1", 10000L),
                new Event("2", "2", 9000L),
                new Event("2", "3", 11000L),
                new Event("3", "3", 12000L),
                new Event("3", "3", 4102000L)
        );

        // out of order stream
        SingleOutputStreamOperator<Event> u = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );

        // all data together
        u.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new UvCountByWindow())
                .print();
        env.execute();
    }
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            for (Event e: iterable){
                userSet.add(e.user);
            }

            int uv = userSet.size();

            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("window " + new Timestamp(start) + " ~ " + new Timestamp(end) +
                    " UV: " + uv);

        }
    }
}
