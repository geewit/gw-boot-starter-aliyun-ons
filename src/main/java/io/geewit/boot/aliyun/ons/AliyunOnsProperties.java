package io.geewit.boot.aliyun.ons;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * aliyun ons properties
 *
 * @author geewit
 */
@ConfigurationProperties(prefix = "aliyun.ons")
public class AliyunOnsProperties {
    /**
     * onsaddr
     */
    private String onsaddr;

    /**
     * sendTimeout
     */
    private String sendTimeout;


    public String getOnsaddr() {
        return onsaddr;
    }

    public void setOnsaddr(String onsaddr) {
        this.onsaddr = onsaddr;
    }

    public String getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(String sendTimeout) {
        this.sendTimeout = sendTimeout;
    }
}
