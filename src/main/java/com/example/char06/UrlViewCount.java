package com.example.char06;

import java.sql.Timestamp;

public class UrlViewCount {
    public String url;
    public long count;
    public long windowStart;
    public long windowEnd;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, long count, long windowStart, long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
