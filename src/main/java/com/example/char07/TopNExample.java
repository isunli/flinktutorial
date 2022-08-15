package com.example.char07;

import com.example.char05.ClickSource;
import com.example.char05.Event;
import com.example.char06.URlCountExample;
import com.example.char06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new URlCountExample.UrlViewCountAgg(),
                        new URlCountExample.UrlViewCountResult());

        urlCountStream.print("url count");

        urlCountStream.keyBy(data -> data.windowEnd)
                        .process(new TopNProcessResult(2))
                                .print();


        env.execute();

    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String>{
        private int n;

        //state
        private ListState<UrlViewCount> urlViewCountListState;
        public TopNProcessResult(int n){
            this.n=n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }
        @Override
        public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
            urlViewCountListState.add(urlViewCount);

            //System.out.println("Created timer");
            System.out.println("current watermark: " + new Timestamp(context.timerService().currentWatermark()));
            context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList< UrlViewCount > urlViewCountArrayList = new ArrayList<>();

            for(UrlViewCount urlViewCount: urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o1.count > o2.count ? 1 : -1;
                }
            });


            StringBuilder res = new StringBuilder();
            res.append("-----------------------\n");
            res.append("current watermark: " + new Timestamp(ctx.timerService().currentWatermark())+"\n");
            res.append("window end at: " + new Timestamp(ctx.getCurrentKey()) + "\n");
            int min = Math.min(2, urlViewCountArrayList.size());
            for (int i = 0; i < min ; i++) {
                UrlViewCount currTuple = urlViewCountArrayList.get(i);
                res.append("No, "+(1+i)+" "+currTuple.url+" #visitor: " + currTuple.count+"\n");
            }
            res.append("-----------------------\n");

            out.collect(res.toString());
        }
    }
}
