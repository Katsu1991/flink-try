package com.gongyu.flink.stream.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author gongyu
 */
public class HDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String dfsPath = "hdfs://mycluster/data/wc/input/";
        TextInputFormat format = new TextInputFormat(new Path(dfsPath));
        try {
            DataStreamSource<String> streamSource = env.readFile(format, dfsPath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10);
            streamSource.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
        env.execute();
    }
}
