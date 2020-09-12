package com.gongyu.flink.stream.tableAndSql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


/**
 * @author gongyu
 */
public class TestTableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<Student> socketStream = streamEnv.socketTextStream("node04", 8888)
                .map(t -> {
                    String[] parts = t.split("\\s");
                    return new Student(Integer.parseInt(parts[0]), parts[1], Integer.parseInt(parts[2]));
                });

        Table table = tableEnv.fromDataStream(socketStream);

        Table value = table.where("clasz=1")
                .select("clasz, name, fraction");

        tableEnv.toRetractStream(value, Student.class).print();

        tableEnv.execute("job1");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student {
        public Integer clasz;
        public String name;
        public Integer fraction;
    }
}
