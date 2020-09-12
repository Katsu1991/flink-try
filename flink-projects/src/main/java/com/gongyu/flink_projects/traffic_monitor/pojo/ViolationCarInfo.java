package com.gongyu.flink_projects.traffic_monitor.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gongyu
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ViolationCarInfo {
    private String carNo;
    private String msg;
    private Long createTime;
}
