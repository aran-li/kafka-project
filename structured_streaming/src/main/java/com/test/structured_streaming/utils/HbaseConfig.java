package com.test.structured_streaming.utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/3/30 18:00
 */
@Configuration
@ConfigurationProperties(prefix = HbaseConfig.CONF_PREFIX)
public class HbaseConfig {
    public static final String CONF_PREFIX = "hbase.conf";

    private Map<String, String> confMaps;

    public Map<String, String> getconfMaps() {
        return confMaps;
    }

    public void setconfMaps(Map<String, String> confMaps) {
        this.confMaps = confMaps;
    }
}
