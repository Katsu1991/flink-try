package com.gongyu.flink.stream.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 超速100 发出警告(利用定时器，延迟2s发送)
 * <p>
 * A0001 121
 *
 * @author gongyu
 */
public class OnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("node04", 8888);

        stream
                .map(t -> {
                    String[] splits = t.split("\\s");
                    return new CarInfo(splits[0], Long.valueOf(splits[1]));
                })
                .keyBy(t -> t.carId)
                .process(new KeyedProcessFunction<String, CarInfo, String>() {
                    @Override
                    public void processElement(CarInfo value, Context ctx, Collector<String> out) throws Exception {
                        long currentTime = ctx.timerService().currentProcessingTime();
                        if (value.speed > 100) {
                            long timerTime = currentTime + 2 * 1000;
                            ctx.timerService().registerProcessingTimeTimer(timerTime);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        String warnMsg = "warn... time" + timestamp + " carId:" + ctx.getCurrentKey();
                        out.collect(warnMsg);
                    }
                })
                .print();
        env.execute();

    }

    @Data
    @AllArgsConstructor
    static class CarInfo {
        private String carId;
        private Long speed;
    }
}
/*
object MonitorOverSpeed02 {
  case class CarInfo(carId:String,speed:Long)
  def main(args: Array[String]): Unit = {
    stream.map(data => {
      val splits = data.split(" ")
      val carId = splits(0)
      val speed = splits(1).toLong
      CarInfo(carId,speed)
}).keyBy(_.carId) //KeyedStream调用process需要传入KeyedProcessFunction //DataStream调用process需要传入ProcessFunction
.process(new KeyedProcessFunction[String,CarInfo,String] {
      override def processElement(value: CarInfo, ctx:
KeyedProcessFunction[String, CarInfo, String]#Context, out:
Collector[String]): Unit = {
        val currentTime = ctx.timerService().currentProcessingTime()
        if(value.speed > 100 ){
          val timerTime = currentTime + 2 * 1000
          ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
}
      override def onTimer(timestamp: Long, ctx:
KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out:
Collector[String]): Unit = {
        var warnMsg = "warn... time:" + timestamp + "  carID:" +
ctx.getCurrentKey
        out.collect(warnMsg)
      }
    }).print()
    env.execute()
} }

 */
