package com.gongyu.flink.stream.conn;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 利用定时器，解决cache文件因只加载一次导致的更新文件后无法加载到程序中
 *
 * @author gongyu
 */
public class CacheFileRefresh {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("node04", 8888);

        socketStream.map(t -> Integer.parseInt(t))
                .map(new RichMapFunction<Integer, String>() {
                    private ConcurrentHashMap<Integer, String> id2CityMap = new ConcurrentHashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ScheduledThreadPoolExecutor scheduledPool = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().namingPattern("scheduler-pool").daemon(true).build());
                        scheduledPool.scheduleAtFixedRate(new Runnable() {
                            @Override
                            public void run() {
                                refreshMap();
                            }
                        }, 2, 2, TimeUnit.SECONDS);
                    }

                    @Override
                    public String map(Integer value) throws Exception {
                        return id2CityMap.getOrDefault(value, "not found...");
                    }

                    private void refreshMap() {
                        try {
                            RandomAccessFile accessFile = new RandomAccessFile("data/id2city", "r");
                            String line = null;
                            while ((line = accessFile.readLine()) != null) {
                                String utf8Str = new String(line.getBytes("ISO-8859-1"), "utf8");
                                String[] values = utf8Str.split("\\s");
                                id2CityMap.put(Integer.valueOf(values[0]), values[1]);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .print();
        env.execute();
    }
}
