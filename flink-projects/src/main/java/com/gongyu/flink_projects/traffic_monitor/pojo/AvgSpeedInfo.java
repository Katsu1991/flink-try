package com.gongyu.flink_projects.traffic_monitor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gongyu
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AvgSpeedInfo {
    ////某个时间范围内卡口的平均车速和通过的车辆数量
    private Long start;
    private long end;
    private String monitorId;
    private Double avgSpeed;
    private Integer carCnt;
}
