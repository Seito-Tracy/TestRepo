package com.seito.capture.dto;

import lombok.Data;

import java.util.List;

/**
 * @ description: WAL日志文件 oldkeys字段
 * @ author: tracy.tan
 * @ create: 2024-01-15 14:28
 **/
@Data
public class WALChangeOldKeys {
    private List<String> keynames;

    private List<String> keytypes;

    private List<Object> keyvalues;
}
