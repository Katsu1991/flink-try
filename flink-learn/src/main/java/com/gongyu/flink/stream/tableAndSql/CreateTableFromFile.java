package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * @author gongyu
 */
public class CreateTableFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        CsvTableSource csvTableSource = new CsvTableSource(
                "data/carId2Name",
                new String[]{"col1", "col2"},
                new TypeInformation[]{Types.STRING, Types.STRING}
        );

        //注册第一张表
        String tableName = "t_test";
        tableEnv.registerTableSource(tableName, csvTableSource);


        //还原一张表
        Table table = tableEnv.scan(tableName);
        table.printSchema();

        Table data = table.where("clo1==='222'")
                .select("col1, col2");

        tableEnv.toRetractStream(data, CsvTableSource.class).print();

        tableEnv.execute("job1");
    }
}
