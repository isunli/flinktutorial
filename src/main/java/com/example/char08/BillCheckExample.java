package com.example.char08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                })
        );


        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order-1", "app", "success", 3000L),
                Tuple4.of("order-3", "app", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f3;
                    }
                })
        );

        appStream.keyBy(data -> data.f0)
                .connect(thirdPartyStream.keyBy(data -> data.f0))
                .process(new OrderMatchResult())
                .print();


        env.execute();
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // state
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
           appEventState = getRuntimeContext().getState(
                   new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
           );
           thirdPartyEventState = getRuntimeContext().getState(
                   new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
           );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3,
                                    CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context,
                                    Collector<String> collector
        ) throws Exception {
            if (thirdPartyEventState.value() != null) {
                collector.collect("success " + stringStringLongTuple3 + " " + thirdPartyEventState.value());

                thirdPartyEventState.clear();
            } else {
                appEventState.update(stringStringLongTuple3);

                // timer to wait for next stream
                context.timerService().registerEventTimeTimer(stringStringLongTuple3.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> stringStringStringLongTuple4, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            if (appEventState.value() != null) {
                collector.collect("success " + stringStringStringLongTuple4 + " " + thirdPartyEventState.value());

                thirdPartyEventState.clear();

            } else {
                thirdPartyEventState.update(stringStringStringLongTuple4);

                // timer to wait for next stream
                context.timerService().registerEventTimeTimer(stringStringStringLongTuple4.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // trigger if one state is not empty
            if (appEventState.value() != null){
                out.collect("Warning: not match" + appEventState.value() + " " + "no third party");
            }

            if(thirdPartyEventState.value() != null) {

                out.collect("Warning: not match" + appEventState.value() + " " + "no app info");
            }

            thirdPartyEventState.clear();
            appEventState.clear();
        }
    }
}
