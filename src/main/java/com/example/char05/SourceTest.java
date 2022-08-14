package com.example.char05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        stream1.print("1");


        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);
        numStream.print("nums");
        ArrayList<Event> events = new ArrayList<Event>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        stream2.print("2");


        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("1","1", 10L),
                new Event("2", "2", 9L)
        );

        stream3.print("3");


//        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);
//        stream4.print("4");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9002");

        // env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));


        env.execute();
    }
}
