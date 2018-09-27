package io.geewit.boot.aliyun.ons.utils;

import com.aliyun.openservices.ons.api.Message;

import java.nio.charset.StandardCharsets;

/**
 * @author geewit
 */
@SuppressWarnings({"unused"})
public class MessageBuilder {

    public static Message build(String topic, String tag, String key, String body) {
        return new Message(topic, tag, key, body.getBytes(StandardCharsets.UTF_8));
    }
}
