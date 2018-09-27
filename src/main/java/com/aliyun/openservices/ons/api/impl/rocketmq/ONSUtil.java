package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageConst;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageAccessor;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class ONSUtil {
    private static final Set<String> ReservedKeySetRMQ = new HashSet<>();

    private static final Set<String> ReservedKeySetONS = new HashSet<>();


    static {

        /**
         * RMQ
         */
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_KEYS);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_TAGS);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_RETRY_TOPIC);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_REAL_TOPIC);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_REAL_QUEUE_ID);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_PRODUCER_GROUP);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_MIN_OFFSET);
        ReservedKeySetRMQ.add(MessageConst.PROPERTY_MAX_OFFSET);

        /**
         * ONS
         */
        ReservedKeySetONS.add(Message.SystemPropKey.TAG);
        ReservedKeySetONS.add(Message.SystemPropKey.KEY);
        ReservedKeySetONS.add(Message.SystemPropKey.MSGID);
        ReservedKeySetONS.add(Message.SystemPropKey.RECONSUMETIMES);
        ReservedKeySetONS.add(Message.SystemPropKey.STARTDELIVERTIME);
        ReservedKeySetONS.add(Message.SystemPropKey.BORNHOST);
        ReservedKeySetONS.add(Message.SystemPropKey.BORNTIMESTAMP);
        ReservedKeySetONS.add(Message.SystemPropKey.SHARDINGKEY);
    }


    public static com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgConvert(com.aliyun.openservices.ons.api.Message message) {
        com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = new com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message();
        if (message == null) {
            throw new ONSClientException("\'message\' is null");
        }

        if (message.getTopic() != null) {
            msgRMQ.setTopic(message.getTopic());
        }
        if (message.getKey() != null) {
            msgRMQ.setKeys(message.getKey());
        }
        if (message.getTag() != null) {
            msgRMQ.setTags(message.getTag());
        }
        if (message.getStartDeliverTime() > 0) {
            msgRMQ.putUserProperty(Message.SystemPropKey.STARTDELIVERTIME, String.valueOf(message.getStartDeliverTime()));
        }
        if (message.getBody() != null) {
            msgRMQ.setBody(message.getBody());
        }

        if (message.getShardingKey() != null && !message.getShardingKey().isEmpty()) {
            msgRMQ.putUserProperty(Message.SystemPropKey.SHARDINGKEY, message.getShardingKey());
        }

        Properties systemProperties = MessageAccessor.getSystemProperties(message);
        if (systemProperties != null) {
            for (Entry<Object, Object> next : systemProperties.entrySet()) {
                if (!ReservedKeySetONS.contains(next.getKey().toString())) {
                    com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor.putProperty(msgRMQ, next.getKey().toString(),
                            next.getValue().toString());
                }
            }
        }

        Properties userProperties = message.getUserProperties();
        if (userProperties != null) {
            for (Entry<Object, Object> next : userProperties.entrySet()) {
                if (!ReservedKeySetRMQ.contains(next.getKey().toString())) {
                    com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageAccessor.putProperty(msgRMQ, next.getKey().toString(),
                            next.getValue().toString());
                }
            }
        }

        return msgRMQ;
    }

    public static com.aliyun.openservices.ons.api.Message msgConvert(com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ) {
        com.aliyun.openservices.ons.api.Message message = new com.aliyun.openservices.ons.api.Message();
        if (msgRMQ.getTopic() != null) {
            message.setTopic(msgRMQ.getTopic());
        }
        if (msgRMQ.getKeys() != null) {
            message.setKey(msgRMQ.getKeys());
        }
        if (msgRMQ.getTags() != null) {
            message.setTag(msgRMQ.getTags());
        }
        if (msgRMQ.getBody() != null) {
            message.setBody(msgRMQ.getBody());
        }

        message.setReconsumeTimes(((MessageExt) msgRMQ).getReconsumeTimes());
        message.setBornTimestamp(((MessageExt) msgRMQ).getBornTimestamp());
        message.setBornHost(String.valueOf(((MessageExt) msgRMQ).getBornHost()));

        Map<String, String> properties = msgRMQ.getProperties();
        if (properties != null) {
            for (Entry<String, String> next : properties.entrySet()) {
                // System
                if (ReservedKeySetRMQ.contains(next.getKey()) || ReservedKeySetONS.contains(next.getKey())) {
                    MessageAccessor.putSystemProperties(message, next.getKey(), next.getValue());
                }
                // User
                else {
                    message.putUserProperties(next.getKey(), next.getValue());
                }
            }
        }

        return message;
    }

    public static Properties extractProperties(final Properties properties) {
        Properties newPro = new Properties();
        Properties inner = null;
        try {
            Field field = Properties.class.getDeclaredField("defaults");
            field.setAccessible(true);
            inner = (Properties) field.get(properties);
        } catch (Exception ignore) {
        }

        if (inner != null) {
            for (final Entry<Object, Object> entry : inner.entrySet()) {
                newPro.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }

        for (final Entry<Object, Object> entry : properties.entrySet()) {
            newPro.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        return newPro;
    }
}
