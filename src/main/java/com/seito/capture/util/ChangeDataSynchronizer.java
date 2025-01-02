package com.seito.capture.util;

import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONUtil;
import com.seito.capture.dto.ChangeDataDTO;
import com.seito.capture.enums.DataChangeTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ description: ES变更数据同步器
 * @ author: tracy.tan
 * @ create: 2024-01-17 12:43
 **/
@Slf4j
@Component
public class ChangeDataSynchronizer {
    private final RestHighLevelClient restHighLevelClient = SpringUtil.getBean(RestHighLevelClient.class);

    private final DataSource dataSource = SpringUtil.getBean(DataSource.class);

    public boolean syncToES(ChangeDataDTO changeDataDTO) {
        String index = changeDataDTO.getIndex();
        StringBuilder docIdSB = new StringBuilder();
        Map<String, Object> keyValue = changeDataDTO.getKeyValue();
        //将primary key根据column name排序 下划线拼接作为ES文档的ID
        keyValue.keySet().stream().sorted().collect(Collectors.toList()).forEach(key -> {
            docIdSB.append(keyValue.get(key)).append("_");
        });
        //去掉最后一个下划线
        String docId = docIdSB.substring(0, docIdSB.length() -1);
        changeDataDTO.getFieldsValue().putAll(keyValue);
        boolean syncStatus = false;
        switch (changeDataDTO.getChangeType()){
            case INSERT:
                IndexRequest indexRequest = new IndexRequest().index(index).id(docId);
                String insertJsonStr = JSONUtil.toJsonStr(changeDataDTO.getFieldsValue());
                indexRequest.source(insertJsonStr, XContentType.JSON);
                try {
                    IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                    syncStatus =  indexResponse.status().equals(RestStatus.CREATED);
                } catch (Exception e) {
                    log.error("fail to sync data to es! data:" + JSONUtil.toJsonStr(changeDataDTO), e);
                    return false;
                }
                break;
            case UPDATE:
                UpdateRequest updateRequest = new UpdateRequest().index(index).id(docId);
                try {
                    String updateJsonStr = JSONUtil.toJsonStr(changeDataDTO.getFieldsValue());
                    updateRequest.doc(updateJsonStr, XContentType.JSON);
                    UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                    syncStatus =  updateResponse.status().equals(RestStatus.OK);
                } catch (Exception e) {
                    log.error("fail to sync data to es! data:" + JSONUtil.toJsonStr(changeDataDTO), e);
                    return false;
                }
                break;
            case DELETE:
                DeleteRequest deleteRequest = new DeleteRequest().index(index).id(docId);
                try {
                    DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
                    syncStatus =  deleteResponse.status().equals(RestStatus.OK);
                }catch (Exception e) {
                    log.error("fail to sync data to es! data:" + JSONUtil.toJsonStr(changeDataDTO), e);
                    return false;
                }
        }
        if (syncStatus) {
            log.info("sync data to elasticsearch successfully, data:{}", JSONUtil.toJsonStr(changeDataDTO));
            return true;
        }
        else {
            log.error("fail to sync data to elasticsearch! data:{}", JSONUtil.toJsonStr(changeDataDTO));
            return false;
        }

    }

    @Async("syncDataTaskExecutor")
    public void retrySyncDataToES(ChangeDataDTO changeDataDTO) {
        DataChangeTypeEnum changeType = changeDataDTO.getChangeType();

        if (DataChangeTypeEnum.DELETE.equals(changeType)) {
            if (syncToES(changeDataDTO)) {
                return;
            }
            while (changeDataDTO.getSyncTimes() < 5) {
                changeDataDTO.setSyncTimes(changeDataDTO.getSyncTimes() + 1);
                this.syncToES(changeDataDTO);
            }
        }
        String sql = "select {column} from {table} where {condition}";
        StringBuilder columnSql = new StringBuilder();
        StringBuilder conditionSql = new StringBuilder();
        List<Object> condition = new ArrayList<>();
        String table = changeDataDTO.getTable();
        for (String columnName : changeDataDTO.getFieldsValue().keySet()) {
            columnSql.append(columnName).append(",");
        }
        for (Map.Entry<String, Object> keyValue : changeDataDTO.getKeyValue().entrySet()) {
            condition.add(keyValue.getValue());
            conditionSql.append("and ").append(keyValue.getKey()).append(" = ? ");
        }
        sql = sql.replace("{column}", columnSql.substring(0, columnSql.length()-1))
                .replace("{table}", table)
                .replace("{condition}", conditionSql.toString().replaceFirst("and ", ""));

        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < condition.size(); i++) {
                preparedStatement.setObject(i+1, condition.get(i));
            }
            log.info("execute sql:{}", preparedStatement.toString());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                for (String key : changeDataDTO.getFieldsValue().keySet()) {
                    changeDataDTO.getFieldsValue().put(key, resultSet.getObject(key));
                }
            }
            else {
                changeDataDTO.setChangeType(DataChangeTypeEnum.DELETE);
            }
            if (syncToES(changeDataDTO)) {
                return;
            }
            while (changeDataDTO.getSyncTimes() < 5) {
                changeDataDTO.setSyncTimes(changeDataDTO.getSyncTimes() + 1);
                this.syncToES(changeDataDTO);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
