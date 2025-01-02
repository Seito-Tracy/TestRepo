package com.seito.capture.dto;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * @ description:
 * @ author: tracy.tan
 * @ create: 2024-01-16 12:15
 **/
@Data
@Builder
public class CaptureTableStruct {
    //需要同步到ES的索引名称
    private String index;

    //需要同步的表字段
    private Set<String> fields;

    //需要同步的表主键
    private Set<String> primaryKeys;

    public boolean isPrimaryKey(String key) {
        return primaryKeys.contains(key);
    }

    public boolean needToCapture(String key) {
        return fields.contains(key);
    }
}
