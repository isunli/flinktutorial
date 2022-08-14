package com.example.char08;

import com.example.char05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SingleOutputStreamOperator<Event> clickStream = env.fromElements(
            new Event("Bob", "1", 2000L),
            new Event("Alice", "2", 3000L),
            new Event("Alice", "2", 3500L),
            new Event("Bob", "1", 2500L),
            new Event("Alice", "2", 36000L),
            new Event("Bob", "1", 30000L),
            new Event("Bob", "1", 23000L),
            new Event("Bob", "1", 33000L)
    ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
    );
    SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
            Tuple2.of("Mary", 5000L),
            Tuple2.of("Alice",5000L),
            Tuple2.of("Bob", 20000L),
            Tuple2.of("Alice", 20000L),
            Tuple2.of("Cary", 51000L)
    ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                        @Override
                        public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                            return stringLongTuple2.f1;
                        }
                    })
    );
    orderStream.keyBy(data -> data.f0)
                    .intervalJoin(clickStream.keyBy(data -> data.user))
                            .between(Time.seconds(-5), Time.seconds(10))
                                    .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                                        @Override
                                        public void processElement(Tuple2<String, Long> left, Event right, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context context, Collector<String> collector) throws Exception {
                                            collector.collect(right + "->" + left);
                                        }
                                    }).print();

    env.execute();
}
}
