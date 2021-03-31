package com.test.structured_streaming.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/3/30 18:17
 */
@Configuration
public class UcareHbaseConfiguration {
    /**
     * 读取HBase的zookeeper地址
     */
    @Value("${hbase.zookeeper.quorum}")
    private String quorum;

    /**
     * 配置HBase连接参数
     *
     * @return
     */
    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfig() {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, quorum);

        return config;
    }
    //每次调用get方法就会创建一个Connection
    @Bean
    public Supplier<Connection> hbaseConnSupplier() {
        return () -> {
            try {
                return hbaseConnection();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    //@Scope标明模式,默认单例模式. prototype多例模式
    //若是在其他类中直接@Autowired引入的,多例就无效了,因为那个类在初始化的时候,已经创建了创建了这个bean了,之后调用的时候,不会重新创建,若是想要实现多例,就要每次调用的时候,手动获取bean
    @Scope(value = "prototype")
    public Connection hbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(hbaseConfig());
    }
}
