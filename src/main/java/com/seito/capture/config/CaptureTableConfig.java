package com.seito.capture.config;

import com.seito.capture.dto.CaptureTableStruct;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @ description: 捕获实体配置类
 * @ author: tracy.tan
 * @ create: 2024-01-16 12:19
 **/
public class CaptureTableConfig {
    //Key为表名 Value为表的结构
    private static final Map<String, CaptureTableStruct> config = new HashMap<>();

    static {
        //配置测试表结构
        Set<String> fieldTablePrimaryKeySet = Stream.of("pk1", "pk2").collect(Collectors.toSet());
        Set<String> fieldTableFieldSet = Stream.of("dt_character", "dt_double", "dt_tsp", "dt_vchar", "dt_bigint", "dt_boolean", "dt_date", "dt_time", "dt_sint", "dt_text", "dt_bytea", "dt_numeric").collect(Collectors.toSet());

        config.put("field_table", CaptureTableStruct.builder().
                index("cdc_test_index").
                primaryKeys(fieldTablePrimaryKeySet).
                fields(fieldTableFieldSet).build());

        //配置c_order表结构
        Set<String> cOrderPrimaryKeySet = Stream.of("outlet", "tran_station","tran_index","order_index").collect(Collectors.toSet());
        Set<String> cOrderFieldSet = Stream.of("table_num", "sales_date","ref_num","sub_ref","ticket_no","nrv_type","smode","price_type","io_type","price_change","item_index","item_code","cat_code","setmeal","setmeal_group","setmeal_order_index","setmeal_qty","org_item_code","qty","sales_amt","price","amount","item_disc").collect(Collectors.toSet());
        config.put("c_order", CaptureTableStruct.builder().
                index("test_c_order_index").
                primaryKeys(cOrderPrimaryKeySet).
                fields(cOrderFieldSet).build());
    }

    //获取表结构
    public static CaptureTableStruct getCaptureTableStruct(String tableName) {
        return config.get(tableName);
    }


    //判断是否需要捕获变化
    public static boolean enableCapture(String tableName) {
        return config.containsKey(tableName);
    }

}
