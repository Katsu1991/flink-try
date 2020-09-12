package com.gongyu.flink.stream.transformation;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.HashMap;

/**
 * 需求：根据车牌号-车主姓名映射表（该表实时动态变化），实时返回进入当前卡口汽车的车主姓名
 *
 * @author gongyu
 */
public class Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String nameFile = "./data/carId2Name";
        //名单数据流, 10ms刷新一次
        DataStreamSource<String> stream1 = env.readFile(new TextInputFormat(new Path(nameFile)), nameFile, FileProcessingMode.PROCESS_CONTINUOUSLY, 10);
        //进入卡口汽车的车牌号数据流
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 8888);

        stream2.connect(stream1).map(new CoMapFunction<String, String, String>() {
            HashMap<String, String> map = new HashMap<>();

            @Override
            public String map1(String key) throws Exception {
                if (map.containsKey(key)) {
                    return map.get(key);
                }
                return key + " 车牌号不存在。。。";
            }

            @Override
            public String map2(String value) throws Exception {
                String[] kv = value.split("\\s");
                map.put(kv[0], kv[1]);
                return value + "加载完毕！！！";
            }
        }).print();

        env.execute();

    }
}
