package com.example.char05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
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

        stream.shuffle().print().setParallelism(4);

        stream.rebalance().print().setParallelism(4);

        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for(int i = 1;i<=8;i++){
                    if(i%2 == getRuntimeContext().getIndexOfThisSubtask())
                        sourceContext.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);
        
        env.fromElements(1,2,3,4,5,6,7,8,9)
                        .partitionCustom(new Partitioner<Integer>() {

                            @Override
                            public int partition(Integer integer, int i) {
                                return integer%2;
                            }
                        }, new KeySelector<Integer, Integer>() {

                            @Override
                            public Integer getKey(Integer integer) throws Exception {
                                return integer;
                            }
                        }).print("custom partition").setParallelism(4);
        env.execute();
    }
}
