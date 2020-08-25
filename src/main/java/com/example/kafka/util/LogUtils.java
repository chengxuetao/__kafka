package com.example.kafka.util;

import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public final class LogUtils {

    public static void changeLogLevel() {
        // 1.logback
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        // 获取应用中的所有logger实例
        List<ch.qos.logback.classic.Logger> loggerList = loggerContext.getLoggerList();

        // 遍历更改每个logger实例的级别,可以通过http请求传递参数进行动态配置
        for (Logger logger : loggerList) {
            logger.setLevel(Level.toLevel("INFO"));
        }
    }

}
