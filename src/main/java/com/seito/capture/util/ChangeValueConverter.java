package com.seito.capture.util;

import cn.hutool.core.date.DateUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @ description: 字段变更值值转换成Java包装类对象
 * @ author: tracy.tan
 * @ create: 2024-01-16 15:00
 **/
@Slf4j
public class ChangeValueConverter {
    public static Map<String, String> TYPE_CONVERT_CLASS = new HashMap<>();

    static {
        TYPE_CONVERT_CLASS.put("bigint", "Long");
        TYPE_CONVERT_CLASS.put("boolean", "Boolean");
        TYPE_CONVERT_CLASS.put("character", "String");
        TYPE_CONVERT_CLASS.put("character varying", "String");
        TYPE_CONVERT_CLASS.put("date", "Date");
        TYPE_CONVERT_CLASS.put("double precision", "Double");
        TYPE_CONVERT_CLASS.put("integer", "Integer");
        TYPE_CONVERT_CLASS.put("text", "String");
        TYPE_CONVERT_CLASS.put("smallint", "Short");
        TYPE_CONVERT_CLASS.put("time without time zone", "String");
        TYPE_CONVERT_CLASS.put("time with time zone", "String");
        TYPE_CONVERT_CLASS.put("timestamp without time zone", "Datetime");
        TYPE_CONVERT_CLASS.put("timestamp with time zone", "Datetime");
        TYPE_CONVERT_CLASS.put("time", "String");
        TYPE_CONVERT_CLASS.put("timestamp", "Datetime");
        TYPE_CONVERT_CLASS.put("numeric", "BigDecimal");
        TYPE_CONVERT_CLASS.put("bytea", "Byte[]");
    }

    public static Object convert(String fieldType, String fieldValue) {
        String javaType = TYPE_CONVERT_CLASS.get(fieldType);
        if (javaType == null) {
            log.error("fail to convert, fieldType:{}, fieldValue:{}", fieldType, fieldValue);
            return fieldValue;
        }
        try {
            switch (javaType) {
                case "Short":
                    return Short.valueOf(fieldValue);
                case "Integer":
                    return Integer.valueOf(fieldValue);
                case "Long":
                    return Long.valueOf(fieldValue);
                case "Boolean":
                    return Boolean.valueOf(fieldValue);
                case "String":
                    return fieldValue.substring(1, fieldValue.length() - 1);
                case "Date":
                    return DateUtil.parseDate(fieldValue.substring(1, fieldValue.length() - 1)).toJdkDate();
                case "Datetime":
                    return DateUtil.parseDateTime(fieldValue.substring(1, fieldValue.length() - 1)).toJdkDate();
                case "Double":
                    return Double.valueOf(fieldValue);
                case "BigDecimal":
                    return new BigDecimal(fieldValue);
                case "Byte[]":
                    return fieldValue.substring(1, fieldValue.length() - 1).getBytes(StandardCharsets.UTF_8);
            }
        } catch (RuntimeException e) {
            log.error("fail to convert, fieldType:{}, fieldValue:{}", fieldType, fieldValue);
            throw e;
        }
        return fieldValue;
    }


}
