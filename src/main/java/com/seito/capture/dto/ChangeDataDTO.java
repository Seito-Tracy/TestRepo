package com.seito.capture.dto;

import com.seito.capture.enums.DataChangeTypeEnum;
import lombok.Data;

import java.util.Map;

/**
 * @ description: 数据变化DTO
 * @ author: tracy.tan
 * @ create: 2024-01-16 11:13
 **/
@Data
public class ChangeDataDTO {
    //同步次数
    private Integer syncTimes = 1;
    private String index;


    private String table;
    private DataChangeTypeEnum changeType;
    private Map<String, Object> fieldsValue;
    private Map<String, Object> keyValue;


}
