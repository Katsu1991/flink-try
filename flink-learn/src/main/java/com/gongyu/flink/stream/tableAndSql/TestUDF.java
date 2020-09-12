package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 自定义sql
 *
 * @author gongyu
 */
public class TestUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        DataStreamSource<String> socketStream = streamEnv.socketTextStream("node04", 8888);

        tableEnv.registerFunction("split", new MyFunction());

        String fields = "line";
        Table table = tableEnv.fromDataStream(socketStream, fields);
        Table result = table.joinLateral("split(line) as (word, count)")
                .groupBy("word")
                .select("word");
        tableEnv.toRetractStream(result, String.class).print();

        tableEnv.execute("job-udf");
    }


    public static class MyFunction extends TableFunction<Row> {
        //定义UDF的返回字段类型,单词作为第一个字段，单词的个数作为第二个字段
        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING, Types.INT);
        }

        public void eval(String line) {
            String[] words = line.split("\\s");
            Row row = new Row(2);
            row.setField(0, words);
            row.setField(1, 1);
            collect(row);
        }
    }
}
