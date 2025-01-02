package com.seito.capture.dto;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * @ description: WAL日志文件变化
 * @ author: tracy.tan
 * @ create: 2024-01-15 14:23
 **/
@Data
@ToString
public class WALChange {
    private String kind;
    private String schema;
    private String table;
    private List<String> columnnames;
    private List<String> columntypes;
    private List<Object> columnvalues;
    private WALChangeOldKeys oldkeys;

}
