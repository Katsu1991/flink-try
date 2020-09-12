package com.gongyu.flink.stream.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author gongyu
 */
public class OrderMonitorByState {
    //在京东里面，一个订单创建之后，15分钟内如果没有支付，会发送一个提示信息给用户，
    // 如果15分钟内已经支付的，需要发一个提示信息给商家
    public static void main(String[] args) throws Exception {
        OrderMonitorByState orderMonitorByState = new OrderMonitorByState();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //创建一个侧输出流
        OutputTag<OrderMessage> tag = new OutputTag<OrderMessage>("pay_timeout"){};
        SingleOutputStreamOperator<OrderInfo> stream = env.readTextFile(orderMonitorByState.getSourcePath())
                .map(line -> {
                    String[] arr = line.split(",");
                    return new OrderInfo(arr[0], arr[1], arr[2], Long.parseLong(arr[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(OrderInfo element) {
                        return element.actionTime * 1000L;
                    }
                });
        SingleOutputStreamOperator<OrderMessage> mainStream = stream.keyBy(value -> value.oid)
                .process(new OrderProcess(tag));
        mainStream.getSideOutput(tag).print("side");
        mainStream.print("main");

        env.execute("OrderMonitorByState");
    }

    private static class OrderProcess extends KeyedProcessFunction<String, OrderInfo, OrderMessage> {
        OutputTag<OrderMessage> tag;
        //状态直接保存订单创建的数据
        ValueState<OrderInfo> createOrderState;
        //状态中还必须保存触发时间
        ValueState<Long> timeOutState;

        public OrderProcess(OutputTag<OrderMessage> tag) {
            this.tag = tag;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            createOrderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo>("create_order", OrderInfo.class));
            timeOutState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time_out", Long.class));
        }

        @Override
        public void processElement(OrderInfo value, Context ctx, Collector<OrderMessage> out) throws Exception {
            //首先从状态中取得订单的创建数据
            OrderInfo createOrder = createOrderState.value();
            if (value.status.equals("create") && createOrder == null) {//刚开始创建订单
                createOrderState.update(value);
                long ts = value.actionTime * 1000L + (15 * 60 * 1000L); //提示信息的触发器  触发的时间
                timeOutState.update(ts);
                //开始注册触发器
                ctx.timerService().registerEventTimeTimer(ts);
            }
            if (value.status.equals("pay") && createOrder != null) {//当前订单创建了，并且支付了
                //判断是否超时
                if (timeOutState.value() > value.actionTime * 1000L) { //未超时
                    //删除触发器
                    ctx.timerService().deleteEventTimeTimer(timeOutState.value());
                    //生成提示信息给商家
                    OrderMessage om = new OrderMessage(value.oid, "pay ok", createOrder.actionTime, value.actionTime);
                    out.collect(om);
                    //清理状态
                    timeOutState.clear();
                    createOrderState.clear();
                }
            }
        }

        //触发器触发的方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderMessage> out) throws Exception {
            OrderInfo createOrder = createOrderState.value();
            if (null != createOrder) {
                ctx.output(tag, new OrderMessage(createOrder.oid, "15mins unpay", createOrder.actionTime, 0L));
                //清理状态
                createOrderState.clear();
                timeOutState.clear();
            }
        }
    }

    public String getSourcePath() {
        return getClass().getResource("/OrderLog.csv").getPath();
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class OrderInfo {
        private String oid;
        private String status;
        private String payId;
        private Long actionTime;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class OrderMessage {
        private String oid;
        private String msg;
        private Long createTime;
        private Long payTime;
    }
}
