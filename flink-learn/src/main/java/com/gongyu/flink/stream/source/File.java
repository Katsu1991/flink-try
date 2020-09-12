package com.gongyu.flink.stream.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author gongyu
 */
public class File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = "./data/carId2Name";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000).print();

        env.execute();
    }
}
