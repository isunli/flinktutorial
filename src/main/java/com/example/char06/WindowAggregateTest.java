package com.example.char06;

import com.example.char05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("1", "1", 10L),
                new Event("2", "2", 9L),
                new Event("2", "3", 11L),
                new Event("3", "3", 12L),
                new Event("3", "3", 412L)
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

        u.keyBy(data->data.user)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                                .aggregate(new AggregateFunction<Event, Tuple2<Long,Integer>, String>() {
                                    @Override
                                    public Tuple2<Long, Integer> createAccumulator() {
                                        return Tuple2.of(0L,0);
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> acc) {
                                        return Tuple2.of(acc.f0+event.timestamp, acc.f1+1);
                                    }

                                    @Override
                                    public String getResult(Tuple2<Long, Integer> acc) {
                                        Timestamp timestamp = new Timestamp(acc.f1 / acc.f1);
                                        return timestamp.toString();
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> acc1, Tuple2<Long, Integer> acc2) {
                                        return Tuple2.of(acc1.f0+acc2.f0, acc1.f1+acc2.f1);
                                    }
                                }).print();



        env.execute();
    }
}
