package com.gongyu.flink_projects.traffic_monitor.monitor;

import com.gongyu.flink_projects.traffic_monitor.pojo.TrafficInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * 抓取各卡口指定时间窗口内的速度topn的车辆
 *
 * @author gongyu
 */
public class TopNSpeed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<TrafficInfo> socketStream = streamEnv.socketTextStream("node04", 8888)
                .map(line -> {
                    String[] arr = line.split(",");
                    return new TrafficInfo(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], Double.valueOf(arr[4]), arr[5], arr[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TrafficInfo>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(TrafficInfo element) {
                        return element.getActionTime();
                    }
                });

        socketStream.keyBy(t -> t.getMonitorId())
                .timeWindow(Time.seconds(5), Time.seconds(2))
                .allowedLateness(Time.seconds(2))
                .process(new ProcessWindowFunction<TrafficInfo, TrafficInfo, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context ctx, Iterable<TrafficInfo> elements, Collector<TrafficInfo> out) throws Exception {
                        TreeSet<TrafficInfo> treeSet = new TreeSet<>(new Comparator<TrafficInfo>() {
                            @Override
                            public int compare(TrafficInfo o1, TrafficInfo o2) {
                                return o1.getSpeed() > o2.getSpeed() ? -1 : (o1.getSpeed().equals(o2.getSpeed()) ? 0 : 1);
                            }
                        });
                        Iterator<TrafficInfo> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            treeSet.add(iterator.next());
                        }

                        int index = 0;
                        for (TrafficInfo trafficInfo : treeSet) {
                            if (index >= 2) {
                                break;
                            }
                            out.collect(trafficInfo);
                            ++index;
                        }
                        System.out.println("时间窗口：" + ctx.window().getStart() + " : " + ctx.window().getEnd());
                    }
                }).print();

        streamEnv.execute();
    }
}
