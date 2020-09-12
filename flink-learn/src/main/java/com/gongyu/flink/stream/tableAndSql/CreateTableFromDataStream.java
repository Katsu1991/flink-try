package com.gongyu.flink.stream.tableAndSql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author gongyu
 */
public class CreateTableFromDataStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<Person1> socktStream = streamEnv.socketTextStream("node04", 8888)
                .map(t -> {
                    String[] parts = t.split("\\s");
                    return new Person1(parts[0], parts[1], Integer.parseInt(parts[2]));
                });

//        tableEnv.fromDataStream(socktStream) //默认直接使用属性名字来作为字段名
        Table t_person = tableEnv.fromDataStream(socktStream);

        t_person.printSchema();
    }



    @Data
    @AllArgsConstructor
    @Builder
    @NoArgsConstructor
    public static class Person1{
        private String name;
        private String addr;
        private Integer age;
    }
}
