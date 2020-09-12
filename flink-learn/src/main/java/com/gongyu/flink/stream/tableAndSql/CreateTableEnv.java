package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 *
 * TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
 * @author gongyu
 */
public class CreateTableEnv {
    public static void main(String[] args) {
        //传统方式
        StreamExecutionEnvironment StreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv1 = StreamTableEnvironment.create(StreamEnv);

        //使用blink 1.8之后
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv2 = StreamTableEnvironment.create(StreamEnv,build);
    }

}
