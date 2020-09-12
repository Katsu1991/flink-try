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
public class TrafficInfo {
    private Long actionTime;
    private String monitorId;
    private String cameraId;
    private String carNo;
    private Double speed;
    private String roadId;
    private String areaId;

}
