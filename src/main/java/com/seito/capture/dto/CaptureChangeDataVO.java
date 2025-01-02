package com.seito.capture.dto;

import com.seito.capture.enums.DataChangeTypeEnum;
import lombok.Data;

import java.util.Map;

/**
 * @description: 捕获变化对象值VO
 * @author: tracy.tan
 * @create: 2024-01-22 12:28
 **/

@Data
public class CaptureChangeDataVO {
    //变化的schema
    private String schema;
    //变化的table
    private String table;
    //变化的类型 INSERT/UPDATE/DELETE
    private DataChangeTypeEnum dataChangeTypeEnum;
    //主键列值
    private Map<String, Object> primaryKeyValueMap;
    //字段列值
    private Map<String, Object> fieldsValueMap;

}
