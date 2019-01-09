package org.apache.hbase.base;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;


@Configuration
@EnableConfigurationProperties(HBaseProperties.class)
public class HBaseConfig {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConfig.class);

    private final HBaseProperties properties;

    public HBaseConfig(HBaseProperties properties) {
        this.properties = properties;
    }

//    @Bean
//    public HbaseTemplate hbaseTemplate() {
//        HbaseTemplate hbaseTemplate = new HbaseTemplate();
//        hbaseTemplate.setConfiguration(getConfiguration());
//        hbaseTemplate.setAutoFlush(true);
//        return hbaseTemplate;
//    }
    @Bean
    public Connection connection() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = this.getConfiguration();
        Connection conn = ConnectionFactory.createConnection(configuration);
        logger.info("获取connectiont连接成功！");
        return conn;
    }

    public  org.apache.hadoop.conf.Configuration getConfiguration() {

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }

        return configuration;
    }
}
