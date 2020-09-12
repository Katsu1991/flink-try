package com.gongyu.flink_projects.traffic_monitor.sink;

import com.gongyu.flink_projects.traffic_monitor.pojo.RepetitionCarWarning;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author gongyu
 */
public class MysqlSink<T> extends RichSinkFunction<T> {

    private Connection conn;
    private PreparedStatement pst;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor?useSSL=false&characterEncoding=utf-8", "root", "root");
    }

    @Override
    public void close() throws Exception {
        pst.close();
        conn.close();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        String sql = null;
        if (value instanceof RepetitionCarWarning) {
            RepetitionCarWarning warning = (RepetitionCarWarning) value;
            sql = getRepetitionCarWarningSql();
            pst = conn.prepareStatement(sql);
            pst.setString(1, warning.getCarNo());
            pst.setString(2, warning.getFirstMonitor());
            pst.setLong(3, warning.getFirstMonitorTime());
            pst.setString(4, warning.getSecondMonitor());
            pst.setLong(5, warning.getSecondMonitorTime());
            pst.setString(6, warning.getMsg());
            pst.setLong(7, warning.getActionTime());
        }

        pst.execute();
    }

    private String getRepetitionCarWarningSql() {
        return "insert into repetition_car_warning values(?, ?, ?, ?, ?, ?, ?)";
    }
}
