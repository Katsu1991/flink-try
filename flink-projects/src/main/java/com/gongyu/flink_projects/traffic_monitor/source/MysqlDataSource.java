package com.gongyu.flink_projects.traffic_monitor.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author gongyu
 */
public class MysqlDataSource extends RichSourceFunction {
    private String sql;
    private Class clazz;

    public MysqlDataSource(String sql, Class clazz) {
        this.sql = sql;
        this.clazz = clazz;
    }

    private boolean flag = true;
    private Connection conn;
    private PreparedStatement pst;
    private ResultSet set;

    int times = 0;
    int cnt = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor?useSSL=false", "root", "root");
        pst = conn.prepareStatement(this.sql);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        times++;
        cnt = 0;

        while (true == flag) {
            set = pst.executeQuery();
            int index;
            Object object;
            while (set.next()) {
                index = 1;
                object = clazz.newInstance();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    String typeName = field.getType().getTypeName();
                    if (field.getType().equals(String.class)) {
                        field.set(object, set.getString(index++));
                    } else if (typeName.contains("double") || typeName.contains("Double")) {
                        field.set(object, set.getDouble(index++));
                    } else if (typeName.contains("float") || typeName.contains("Float")) {
                        field.set(object, set.getFloat(index++));
                    } else if (typeName.contains("long") || typeName.contains("Long")) {
                        field.set(object, set.getLong(index++));
                    } else if (typeName.contains("int") || typeName.contains("Integer")) {
                        field.set(object, set.getInt(index++));
                    } else if (typeName.contains("short") || typeName.contains("Short")) {
                        field.set(object, set.getShort(index++));
                    } else if (typeName.contains("byte") || typeName.contains("Byte")) {
                        field.set(object, set.getByte(index++));
                    } else if (typeName.contains("bool") || typeName.contains("Boolean")) {
                        field.set(object, set.getBoolean(index++));
                    } else {
                        field.set(object, set.getObject(index++));
                    }
                }
                ctx.collect(object);
                cnt++;
            }
            //每隔5s刷新一次
            Thread.sleep(5000);
            set.close();
        }
    }


    @Override
    public void cancel() {
        flag = false;
    }


    @Override
    public void close() throws Exception {
        pst.close();
        conn.close();
    }

}
