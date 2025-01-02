package com.seito.capture.decoding;


import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;

import com.seito.capture.config.ThreadInitializer;
import com.seito.capture.util.ChangeDataSynchronizer;
import com.seito.capture.dto.ChangeDataDTO;
import com.seito.capture.enums.DataChangeTypeEnum;
import com.seito.capture.config.CaptureTableConfig;
import com.seito.capture.util.ChangeValueConverter;
import com.seito.capture.dto.CaptureTableStruct;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;


import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @ description: 数据变化捕获
 * @ author: tracy.tan
 * @ create: 2024-01-09 16:58
 **/
@Slf4j
public class ChangeDataCaptureTestDecoding implements Runnable {
    private static final Set<String> IGNORE_MSG;

    static {
        IGNORE_MSG = new HashSet<>();
        IGNORE_MSG.add("BEGIN");
        IGNORE_MSG.add("COMMIT");
        IGNORE_MSG.add("TRUNCATE");
    }

    @Override
    public void run() {
        boolean shouldStop = false;
        String url = SpringUtil.getProperty("spring.datasource.url");
        Properties props = new Properties();
        PGProperty.USER.set(props, SpringUtil.getProperty("spring.datasource.user"));
        PGProperty.PASSWORD.set(props, SpringUtil.getProperty("spring.datasource.password"));
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, SpringUtil.getProperty("spring.datasource.version"));
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");


        try (Connection con = DriverManager.getConnection(url, props)) {
            JSONConfig logJsonCfg = JSONConfig.create().setIgnoreNullValue(false);
            PGConnection replConnection = con.unwrap(PGConnection.class);
            PGReplicationStream stream =
                    replConnection.getReplicationAPI()
                            .replicationStream()
                            .logical()
                            .withSlotName("test_decoding_slot")
                            .withSlotOption("include-xids", false)
                            .withSlotOption("skip-empty-xacts", true)
                            .start();
            ChangeDataSynchronizer changeDataSynchronizer = SpringUtil.getBean(ChangeDataSynchronizer.class);

            while (!shouldStop) {
                String decodingStr = null;
                try {
                    ByteBuffer msg = stream.readPending();

                    if (msg == null) {
                        TimeUnit.MILLISECONDS.sleep(1L);
                        continue;
                    }
                    long startTime = System.nanoTime();

                    int offset = msg.arrayOffset();
                    byte[] source = msg.array();
                    int length = source.length - offset;
                    decodingStr = new String(source, offset, length);

                    if (!IGNORE_MSG.contains(decodingStr)) {
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
                            ChangeDataDTO changeDataDTO = new ChangeDataDTO();
                            changeDataDTO.setChangeType(dataChangeTypeEnum);
                            changeDataDTO.setIndex(captureTableStruct.getIndex());
                            changeDataDTO.setTable(table);
                            changeDataDTO.setKeyValue(pkMap);
                            changeDataDTO.setFieldsValue(fieldMap);

                            //changeDataDTO为解析捕捉到的整个变化


                            double executionTime = (System.nanoTime() - startTime) / 1000000000.0;

                            log.info("capture data change, schema:{}, table:{}, changeType:{}, primaryKey:{}, field:{}, cost:{} seconds"
                                    ,schema, table, dataChangeTypeEnum.name(), JSONUtil.toJsonStr(pkMap, logJsonCfg), JSONUtil.toJsonStr(fieldMap, logJsonCfg), String.format("%.5f", executionTime));

                            //同步到ES
                            if (!changeDataSynchronizer.syncToES(changeDataDTO)) {
                                //更新失败 重试更新
                                changeDataSynchronizer.retrySyncDataToES(changeDataDTO);
                            }
                        }
                    }

                    //直接消费掉此日志记录
                    stream.setAppliedLSN(stream.getLastReceiveLSN());
                    stream.setFlushedLSN(stream.getLastReceiveLSN());
                    //取消string引用 回收该string
                    decodingStr = null;
                }
                catch (PSQLException e) {
                    log.error("capture data database error", e);
                    shouldStop = true;
                    //重新启动线程
                    ThreadInitializer.initThread();
                }
                catch (Exception e){
                    log.error("capture data unknown error, decode string:" + decodingStr, e);
                }
            }
        } catch (Exception e) {
            log.error("fail to connect postgresql", e);
            //数据库连接失败 或 复制槽读取流异常 重试启动线程
            ThreadInitializer.initThread();
        }
    }
}
