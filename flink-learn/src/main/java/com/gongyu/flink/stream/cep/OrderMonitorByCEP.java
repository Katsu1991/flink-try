package com.gongyu.flink.stream.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author gongyu
 */
public class OrderMonitorByCEP {
    public static void main(String[] args) throws Exception {
        //在京东里面，一个订单创建之后，15分钟内如果没有支付，会发送一个提示信息给用户，
        // 如果15分钟内已经支付的，需要发一个提示信息给商家
        OrderMonitorByState orderMonitorByState = new OrderMonitorByState();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<OrderMonitorByState.OrderMessage> tag = new OutputTag<OrderMonitorByState.OrderMessage>("side tag") {
        };

        //1.定义数据源
        SingleOutputStreamOperator<OrderMonitorByState.OrderInfo> stream = env.readTextFile(orderMonitorByState.getSourcePath())
                .map(line -> {
                    String[] arr = line.split(",");
                    return new OrderMonitorByState.OrderInfo(arr[0], arr[1], arr[2], Long.parseLong(arr[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMonitorByState.OrderInfo>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(OrderMonitorByState.OrderInfo element) {
                        return element.getActionTime() * 1000L;
                    }
                });

        //2。定义Pattern
        Pattern pattern = Pattern.<OrderMonitorByState.OrderInfo>begin("begin")
                .where(new IterativeCondition<OrderMonitorByState.OrderInfo>() {
                    @Override
                    public boolean filter(OrderMonitorByState.OrderInfo value, Context<OrderMonitorByState.OrderInfo> ctx) throws Exception {
                        return "create".equalsIgnoreCase(value.getStatus());
                    }
                })
                .followedBy("second")
                .where(new IterativeCondition<OrderMonitorByState.OrderInfo>() {
                    @Override
                    public boolean filter(OrderMonitorByState.OrderInfo value, Context<OrderMonitorByState.OrderInfo> ctx) throws Exception {
                        return "pay".equalsIgnoreCase(value.getStatus());
                    }
                })
                .within(Time.minutes(15));

        //3。检测事件流
        PatternStream<OrderMonitorByState.OrderInfo> pp = CEP.pattern(stream.keyBy(t -> t.getOid()), pattern);

        //4。选择数据
//        PatternTimeoutFunction<OrderMonitorByState.OrderInfo, OrderMonitorByState.OrderMessage> timeoutFunction = new PatternTimeoutFunction<OrderMonitorByState.OrderInfo, OrderMonitorByState.OrderMessage>() {
//            @Override
//            public OrderMonitorByState.OrderMessage timeout(Map<String, List<OrderMonitorByState.OrderInfo>> map, long timeoutTimestamp) throws Exception {
//                OrderMonitorByState.OrderInfo order = map.get("begin").iterator().next();
//                return new OrderMonitorByState.OrderMessage(order.getOid(), "15mins unpay", order.getActionTime(), 0L);
//            }
//        };
//        PatternSelectFunction<OrderMonitorByState.OrderInfo, OrderMonitorByState.OrderMessage> selectFunction = new PatternSelectFunction<OrderMonitorByState.OrderInfo, OrderMonitorByState.OrderMessage>() {
//            @Override
//            public OrderMonitorByState.OrderMessage select(Map<String, List<OrderMonitorByState.OrderInfo>> map) throws Exception {
//                OrderMonitorByState.OrderInfo create = map.get("begin").iterator().next();
//                OrderMonitorByState.OrderInfo pay = map.get("second").iterator().next();
//                return new OrderMonitorByState.OrderMessage(create.getOid(), "pay ok", create.getActionTime(), pay.getActionTime());
//            }
//        };

        SingleOutputStreamOperator mainSteram = pp.select(
                tag,
                (timeoutMap, timeoutTimestamp) -> {
                    OrderMonitorByState.OrderInfo order = timeoutMap.get("begin").iterator().next();
                    return new OrderMonitorByState.OrderMessage(order.getOid(), "15mins unpay", order.getActionTime(), 0L);
                },
                selectMap -> {
                    OrderMonitorByState.OrderInfo create = selectMap.get("begin").iterator().next();
                    OrderMonitorByState.OrderInfo pay = selectMap.get("second").iterator().next();
                    return new OrderMonitorByState.OrderMessage(create.getOid(), "pay ok", create.getActionTime(), pay.getActionTime());
                }
        );

        mainSteram.getSideOutput(tag).print("side");
        mainSteram.print("main");

        env.execute("OrderMonitorByCEP");
    }
}
