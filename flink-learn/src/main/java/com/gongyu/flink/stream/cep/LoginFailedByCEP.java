package com.gongyu.flink.stream.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author gongyu
 */
public class LoginFailedByCEP {
    //实时的根据用户登录日志，来判断哪些用户是恶意登录
    //恶意登录：10分钟内，连续登录失败3次以上。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.准备数据源
        SingleOutputStreamOperator<LoginEvent> stream = env
                .fromCollection(getDataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.loginTime;
                    }
                });

        //2.定义pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equalsIgnoreCase(value.loginType);
                    }
                })
                .timesOrMore(3)
                .greedy()
                //测试使用10s代替
                .within(Time.seconds(10));

        //2.定义pattern
   /*     Pattern<LoginEvent, LoginEvent> pattern1 = Pattern.<LoginEvent>begin("start")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equalsIgnoreCase(value.loginType);
                    }
                })
                .next("next1")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equalsIgnoreCase(value.loginType);
                    }
                })
                .next("next2")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equalsIgnoreCase(value.loginType);
                    }
                });*/

        //3.检测数据
        PatternStream<LoginEvent> ps = CEP.pattern(stream.keyBy(t -> t.username), pattern);

        //4.选择数据并且返回
        SingleOutputStreamOperator<String> result = ps.select(t -> {
            StringBuilder sb = new StringBuilder();
            List<LoginEvent> list = t.get("start");
            sb.append("用户名:").append(list.get(0).username).append(" 恶意登录，");
            for (LoginEvent event : list) {
                sb.append("登录失败的id时间是:").append(event.id);
            }
            return sb.toString();
        });
        result.print();
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LoginEvent {
        private Long id;
        private String username;
        private String loginType;
        private Long loginTime;

        @Override
        public int hashCode() {
            return username.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (null == object) {
                return false;
            }
            if (!(object instanceof LoginEvent)) {
                return false;
            }
            LoginEvent loginEvent = (LoginEvent) object;
            if (this.getUsername().equalsIgnoreCase(loginEvent.getUsername())) {
                return true;
            }
            return false;
        }
    }

    public static List<LoginEvent> getDataSource() {
        return Stream.of(
                new LoginEvent(1L, "zhangsan", "fail", 1577080469000L),
                new LoginEvent(8L, "wangwu", "fail", 1577080479000L),
                new LoginEvent(2L, "zhangsan", "fail", 1577080470000L),
                new LoginEvent(3L, "zhangsan", "fail", 1577080472000L),
                new LoginEvent(4L, "lisi", "fail", 1577080469000L),
                new LoginEvent(5L, "lisi", "success", 1577080473000L),
                new LoginEvent(6L, "zhangsan", "success", 1577080477000L),
                new LoginEvent(7L, "zhangsan", "fail", 1577080478000L)
        ).collect(Collectors.toList());
    }
}
