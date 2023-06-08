package com.yhl.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConfig {

    public static Configuration getHBaseConf(String env){

        Configuration configuration = HBaseConfiguration.create();
        if("prod".equals(env)){
            configuration.set("hbase.zookeeper.quorum", "nod2.newchinalife.com,nod4.newchinalife.com,nod5.newchinalife.com");
        }else if("test".equals(env)){
            configuration.set("hbase.zookeeper.quorum", "");
        }else if("dev".equals(env)){
            configuration.set("hbase.zookeeper.quorum", "");
        }
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //连接超时时间
        configuration.setInt("hbase.rpc.timeout", 1200000);
        configuration.setInt("hbase.client.operation.timeout", 960000);
        //失败时重试次数
        configuration.set("hbase.client.retries.number", "10");
        //失败重试时等待时间
        configuration.setInt("hbase.client.pause", 120000);
        configuration.setInt("hbase.client.scanner.timeout.period", 200000);

        return configuration;
    }
}
