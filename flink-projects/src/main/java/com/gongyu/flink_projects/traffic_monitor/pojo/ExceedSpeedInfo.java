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
public class ExceedSpeedInfo {
    private String carNo;
    private String areaId;
    private String roadId;
    private String monitorId;
    private Double speed;
    private Integer limitSpeed;
    private Long actionTime;
}
