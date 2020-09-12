package com.gongyu.flink_projects.traffic_monitor.pojo;

import lombok.*;

/**
 * @author gongyu
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MonitorInfo {
    private String areaId;
    private String roadId;
    private String monitorId;
    private int limitSpeed;
}
