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
public class RepetitionCarWarning {
    private String carNo;
    private String firstMonitor;
    private Long firstMonitorTime;
    private String secondMonitor;
    private Long secondMonitorTime;
    private String msg;
    private Long actionTime;
}
