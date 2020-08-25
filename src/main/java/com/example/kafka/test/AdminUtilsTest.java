package com.example.kafka.test;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.security.JaasUtils;

import com.example.kafka.util.LogUtils;

public class AdminUtilsTest {

    public static void main(String[] args) throws Exception {
        LogUtils.changeLogLevel();
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        overviewProperties(zkUtils);
        zkUtils.close();
    }

    @SuppressWarnings("unused")
    private static void deleteTopic(ZkUtils zkUtils) {
        AdminUtils.deleteTopic(zkUtils, "test");
    }

    @SuppressWarnings({ "rawtypes" })
    private static void overviewProperties(ZkUtils zkUtils) throws Exception {
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "topic-test");
        // 查询topic-test属性
        Iterator it = props.entrySet().iterator();
        System.out.println("\t\t\t>size = " + props.size());
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Object key = entry.getKey();
            Object val = entry.getValue();
            System.out.println("\t\t\t>" + key + " = " + val);
        }
    }
}
