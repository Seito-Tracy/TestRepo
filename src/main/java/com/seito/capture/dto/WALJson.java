package com.seito.capture.dto;

import lombok.Data;

import java.util.List;

/**
 * @ description: WAL解析JSON
 * @ author: tracy.tan
 * @ create: 2024-01-15 14:41
 **/
@Data
public class WALJson {
    private List<WALChange> change;
}
