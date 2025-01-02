package com.seito.capture.config;

import com.seito.capture.decoding.ChangeDataCaptureTestDecoding;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @ description: 初始化监控数据变化线程
 * @ author: tracy.tan
 * @ create: 2024-01-17 11:52
 **/
@Slf4j
@Component
public class ThreadInitializer {
    @PostConstruct
    public static void initThread() {
        log.info("starting capture thread...");
        try {
            Thread thread = new Thread(new ChangeDataCaptureTestDecoding());
            thread.setName("CDC Thread");
            thread.setDaemon(true);
            thread.start();
        } catch (Exception e){
            log.error("fail to start capture data change thread", e);
            System.exit(0);
        }
        log.info("start capture thread successfully");
    }
}
