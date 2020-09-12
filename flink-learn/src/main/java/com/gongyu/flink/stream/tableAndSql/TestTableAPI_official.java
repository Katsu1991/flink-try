package com.gongyu.flink.stream.tableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author gongyu
 */
public class TestTableAPI_official {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);


        SingleOutputStreamOperator<Tuple6> stream = streamEnv.socketTextStream("node04", 8888)
                .map(t -> {
                    String[] parts = t.split(",");
                    return new Tuple6(parts[0], parts[1], parts[2], parts[3], Long.parseLong(parts[4]), Integer.parseInt(parts[5]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.INT));

        Table table = tableEnv.fromDataStream(stream, "sid, callOut, callIn, callStatus, callTime, duration");
        Table result = table.filter("callStatus='success'")
//                .groupBy("sid")
                .as("a,b,c,d,e,f")
                .distinct()
                .addColumns("concat(f, 'giaogiao')")
                .select("*");

        TypeInformation[] types = {Types.STRING, Types.INT};
        tableEnv.toRetractStream(result, Row.class).print();

        tableEnv.execute("TestTableAPI_official");
    }
}
