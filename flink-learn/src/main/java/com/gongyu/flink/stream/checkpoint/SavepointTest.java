package com.gongyu.flink.stream.checkpoint;

/**
 * @author gongyu
 *
 * 手动实现checkpoint功能，主要用于系统升级等，需要手动先保存状态，再停止job
 * 使用CheckpointTest 中的代码（checkpoint配置可去除）

 * 1. flink run -c com.gongyu.flink.checkpoint.CheckpointTest -d hadoop-hdfs-1.0-SNAPSHOT.jar
 * 2.进行测试
 * 3.手动保存:flink savepoint [job_id] hdfs://mycluster/flink/sasa,  注意：后面的目录可通过flink-conf.yaml文件中的state.savepoints.dir配置项进行配置（配置后可省略）
 * 4.删除任务
 * 5.重新启动：其中-s 后面的参数来源余第3步保存时，日志中打印的hdfs路径
 *  flink run -c com.gongyu.flink.checkpoint.CheckpointTest -d -s hdfs://mycluster/flink/checkpoint/9a3c353d4e2c977ebd231749ad17f3ae/chk-127 hadoop-hdfs-1.0-SNAPSHOT.jar
 * 6.查看结果，会接着上次的结果继续累加
 *
 */
public class SavepointTest {
}
