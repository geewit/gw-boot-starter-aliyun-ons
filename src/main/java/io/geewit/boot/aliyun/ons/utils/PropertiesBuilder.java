package io.geewit.boot.aliyun.ons.utils;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import io.geewit.boot.aliyun.AliyunProperties;
import io.geewit.boot.aliyun.ons.AliyunOnsProperties;

import java.util.Properties;

/**
 * @author geewit
 */
@SuppressWarnings({"unused"})
public class PropertiesBuilder {
    public static Properties properties(AliyunProperties aliyunProperties, AliyunOnsProperties aliyunOnsProperties) {
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.AccessKey, aliyunProperties.getKey());
        properties.put(PropertyKeyConst.SecretKey, aliyunProperties.getSecret());
        properties.put(PropertyKeyConst.ONSAddr, aliyunOnsProperties.getOnsaddr());
        properties.put(PropertyKeyConst.SendMsgTimeoutMillis, aliyunOnsProperties.getSendTimeout());
        return properties;
    }
}
