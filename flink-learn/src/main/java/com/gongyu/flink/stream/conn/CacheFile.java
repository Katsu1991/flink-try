package com.gongyu.flink.stream.conn;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;

/**
 * 输入001， 然后到文件中匹配对应的城市，文件只会败taskManager加载一次，故文件更新后， 程序读取不到
 *
 * @author gongyu
 */
public class CacheFile {
    public static void main(String[] args) throws Exception {
        String filename = "id2city";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.registerCachedFile("./data/id2city", filename);
        DataStreamSource<String> socketTextStream = env.socketTextStream("node04", 8888);
        socketTextStream
                .keyBy(t -> t)
                .map(new RichMapFunction<String, String>() {
                    MapState<String, String> id2CityMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor descriptor = new MapStateDescriptor("id2CityMapDescriptor", String.class, String.class);
                        id2CityMapState = getRuntimeContext().getMapState(descriptor);

                        System.out.println("初始化map数据。。。");
                        File file = getRuntimeContext().getDistributedCache().getFile(filename);
                        String content = FileUtils.readFileUtf8(file);
                        String[] lines = content.split("\n");
                        for (int i = 0; i < lines.length; i++) {
                            String[] values = lines[i].split("\\s");
                            id2CityMapState.put(values[0], values[1]);
                        }
                    }

                    @Override
                    public String map(String key) throws Exception {
                        Object value = id2CityMapState.get(key);
                        if (null != value) {
                            return value.toString();
                        }
                        return "not find...";
                    }
                }).print();

        env.execute();
    }
}
