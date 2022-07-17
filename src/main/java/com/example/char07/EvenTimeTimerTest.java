package com.example.char07;

import com.example.char05.ClickSource;
import com.example.char05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EvenTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        Long currTs = context.timestamp();
                        collector.collect(context.getCurrentKey() + " data arrived at: " + new Timestamp(currTs) + " watermark: " + context.timerService().currentWatermark());


                        context.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " timer triggered at: " + new Timestamp(timestamp) + ", watermark: "+ ctx.timerService().currentWatermark() );
                    }
                }).print();
        env.execute();
    }

    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            sourceContext.collect(new Event("Mary", "./home", 1000L));
            Thread.sleep(5000L);
            sourceContext.collect(new Event("Alice", "./home", 11000L));
            Thread.sleep(5000L);
            sourceContext.collect(new Event("Bob", "./home", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {

        }
    }

}
