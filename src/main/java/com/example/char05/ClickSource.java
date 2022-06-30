package com.example.char05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // random
        Random random = new Random();

        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100"};
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;

        private Random random = new Random();


        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while(running){
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
