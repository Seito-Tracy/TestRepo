package com.seito.capture.decoding;

import cn.hutool.json.JSONUtil;
import com.seito.capture.config.CaptureTableConfig;
import com.seito.capture.dto.CaptureChangeDataVO;
import com.seito.capture.dto.CaptureTableStruct;
import com.seito.capture.enums.DataChangeTypeEnum;
import com.seito.capture.util.ChangeValueConverter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ description: 逻辑解码插件test_decoding 解析器
 * @ author: tracy.tan
 * @ create: 2024-01-22 12:18
 **/
@Slf4j
public class TestDecodingResolver {
    private Set<String> IGNORE_SCHEMA = new HashSet<>();

    private Set<String> IGNORE_TABLE = new HashSet<>();

    private static final Set<String> IGNORE_MSG;

    static {
        IGNORE_MSG = new HashSet<>();
        IGNORE_MSG.add("BEGIN");
        IGNORE_MSG.add("COMMIT");
        IGNORE_MSG.add("TRUNCATE");
    }

    public void setIgnoreInfo(Set<String> ignoreSchema, Set<String> ignoreTable) {
        this.IGNORE_SCHEMA = ignoreSchema;
        this.IGNORE_TABLE = ignoreTable;
    }

    public void setIgnoreSchema(Set<String> ignoreSchema) {
        this.IGNORE_SCHEMA = ignoreSchema;
    }

    public void setIgnoreTable(Set<String> ignoreTable) {
        this.IGNORE_TABLE = ignoreTable;
    }

    public CaptureChangeDataVO resolve(String decodingStr) {
        if (!IGNORE_MSG.contains(decodingStr)) {
            long startTime = System.nanoTime();
            //处理捕获到的数据变动 并且同步更新到ElasticSearch中
            String[] decodingSplit = decodingStr.split(":", 3);

            //获取schema table 信息
            String[] schemaTable = decodingSplit[0].replace("table ", "").split("\\.");
            String schema = schemaTable[0];
            String table = schemaTable[1];
            CaptureTableStruct captureTableStruct = CaptureTableConfig.getCaptureTableStruct(table);
            //判断是否需要捕获数据变化
            if (captureTableStruct != null) {
                //获取数据变更类型：INSERT UPDATE DELETE
                String changeTypeStr = decodingSplit[1].replace(" ", "");
                DataChangeTypeEnum dataChangeTypeEnum = DataChangeTypeEnum.valueOf(changeTypeStr);

                //获取数据变更字段值
                String fieldsValues = decodingSplit[2].replaceFirst(" ", "");
                //获取表的结构

                //收集主键值的Map
                Map<String, Object> pkMap = new HashMap<>();
                //收集字段值的Map
                Map<String, Object> fieldMap = new HashMap<>();

                //提取字段值
                for (int i = 0; i < fieldsValues.length();i++){
                    StringBuilder fieldNameSB = new StringBuilder();
                    while (fieldsValues.charAt(i) != '[') {
                        fieldNameSB.append(fieldsValues.charAt(i));
                        i++;
                    }
                    //跳过1个符号 [
                    i++;
                    String fieldName = fieldNameSB.toString();
                    StringBuilder fieldTypeSB = new StringBuilder();
                    while (fieldsValues.charAt(i) != ']') {
                        fieldTypeSB.append(fieldsValues.charAt(i));
                        i++;
                    }
                    //跳过2个符号 ]:
                    i=i+2;
                    String fieldType = fieldTypeSB.toString();
                    StringBuilder fieldValueSB = new StringBuilder();
                    while (i < fieldsValues.length()) {
                        if (fieldsValues.charAt(i) != ' ') {
                            fieldValueSB.append(fieldsValues.charAt(i));
                            i++;
                            continue;
                        }
                        if (fieldValueSB.toString().startsWith("'")) {
                            if (fieldValueSB.toString().endsWith("'") && fieldValueSB.length() > 1) {
                                break;
                            }
                            else {
                                fieldValueSB.append(fieldsValues.charAt(i));
                                i++;
                            }
                        }else {
                            break;
                        }
                    }
                    String fieldValue = fieldValueSB.toString();
                    Object bean = null;
                    if (!fieldValue.equals("null")) {
                        bean = ChangeValueConverter.convert(fieldType, fieldValue);
                    }

                    if (captureTableStruct.isPrimaryKey(fieldName)) {
                        pkMap.put(fieldName, bean);
                    }
                    else if (captureTableStruct.needToCapture(fieldName)){
                        fieldMap.put(fieldName, bean);
                    }
                }
                CaptureChangeDataVO captureChangeDataVO = new CaptureChangeDataVO();
                captureChangeDataVO.setSchema(schema);
                captureChangeDataVO.setTable(table);
                captureChangeDataVO.setPrimaryKeyValueMap(pkMap);
                captureChangeDataVO.setFieldsValueMap(fieldMap);
                captureChangeDataVO.setDataChangeTypeEnum(dataChangeTypeEnum);
                double executionTime = (System.nanoTime() - startTime) / 1000000000.0;
                log.info("capture data change, data:{}, cost:{} seconds"
                        ,JSONUtil.toJsonStr(captureChangeDataVO), String.format("%.5f", executionTime));
                return captureChangeDataVO;
            }
        }
        return null;
    }




}
