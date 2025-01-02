package com.seito.capture.decoding;


import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONUtil;
import com.seito.capture.dto.WALChange;
import com.seito.capture.dto.WALJson;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ description: 数据变化捕获
 * @ author: tracy.tan
 * @ create: 2024-01-09 16:58
 **/
@Slf4j
public class ChangeDataCaptureWal2Json implements Runnable {

    @Override
    public void run() {

        String url = SpringUtil.getProperty("spring.datasource.url");
        Properties props = new Properties();
        PGProperty.USER.set(props, SpringUtil.getProperty("spring.datasource.user"));
        PGProperty.PASSWORD.set(props, SpringUtil.getProperty("spring.datasource.password"));
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, SpringUtil.getProperty("spring.datasource.version"));
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");


        try (Connection con = DriverManager.getConnection(url, props)) {
            PGConnection replConnection = con.unwrap(PGConnection.class);

            PGReplicationStream stream =
                    replConnection.getReplicationAPI()
                            .replicationStream()
                            .logical()
                            .withSlotName("test_slot")
                            .withSlotOption("include-xids", false)
                            .start();


            while (true) {
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    TimeUnit.MILLISECONDS.sleep(1L);
                    continue;
                }

                int offset = msg.arrayOffset();
                byte[] source = msg.array();
                int length = source.length - offset;
                String walJsonString = new String(source, offset, length);

                WALJson walChange = JSONUtil.toBean(walJsonString, WALJson.class);
                List<WALChange> changeList = walChange.getChange();
                System.out.println(JSONUtil.toJsonStr(changeList));
                //TODO 处理捕获到的数据变动 并且同步更新到ElasticSearch中

                //ES同步更新后 数据库记录处理到的最大LSN 下次从这里开始同步
                stream.setAppliedLSN(stream.getLastReceiveLSN());
                stream.setFlushedLSN(stream.getLastReceiveLSN());
            }

        } catch (SQLException | InterruptedException e) {
            //失败后定期尝试重连数据库
            throw new RuntimeException(e);
        }
    }
}
