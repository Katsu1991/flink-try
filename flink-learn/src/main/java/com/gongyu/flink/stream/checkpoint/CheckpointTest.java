package com.gongyu.flink.stream.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gongyu
 *
 * 1. flink run -c com.gongyu.flink.checkpoint.CheckpointTest -d hadoop-hdfs-1.0-SNAPSHOT.jar
 * 2.进行测试
 * 3.到hdfs目录下查看checkpoint目录，会存在flink running job对应的id的目录，以及其下的chk-xx文件
 * 4.删除任务
 * 5.重新启动：其中9a3c353d4e2c977ebd231749ad17f3ae就是上一步查看的id
 *  flink run -c com.gongyu.flink.checkpoint.CheckpointTest -d -s hdfs://mycluster/flink/checkpoint/9a3c353d4e2c977ebd231749ad17f3ae/chk-127 hadoop-hdfs-1.0-SNAPSHOT.jar
 * 6.查看结果，会接着上次的结果继续累加
 *
 * 注意：
 * 如果前后2次升级，代码变化比较大，仅仅通过checkpoint（或savepoint）无法保证数据不丢失（设置第二次无法启动），因为代码会为每个算子自动生成一个uid；
 *      因此当出现这样的情况，需要在代码升级前为每个有状态的算子手动设置一个uid
 *
 *
 */
public class CheckpointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint设置
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        //设置前后两次checkpoint的最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(600);
        //设置当任务被取消，仍然保留checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置状态后端，即checkpoint数据保存位置
        env.setStateBackend(new FsStateBackend("hdfs://mycluster/flink/checkpoint", true));
        //下面掩饰另外2中backend
        //env.setStateBackend(new MemoryStateBackend(1024 * 1024 * 1024)); //1G, 默认是这个，可不显示配置
        //env.setStateBackend(new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint", true));
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);
        stream
                .flatMap((String line, Collector<String> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(t -> new Tuple2(t, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
