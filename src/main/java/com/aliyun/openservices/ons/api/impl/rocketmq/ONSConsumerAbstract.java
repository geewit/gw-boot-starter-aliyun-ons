package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.ons.api.MessageSelector;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsConsumeMessageHookImpl;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ONSConsumerAbstract extends ONSClientAbstract {
    private final static Logger log = LoggerFactory.getLogger("AliyunONS-client");
    final DefaultMQPushConsumer defaultMQPushConsumer;
    private final static int MAX_CACHED_MESSAGE_SIZE_IN_MIB = 2048;
    private final static int MIN_CACHED_MESSAGE_SIZE_IN_MIB = 16;
    private final static int MAX_CACHED_MESSAGE_AMOUNT = 50000;
    private final static int MIN_CACHED_MESSAGE_AMOUNT = 100;
    private int maxCachedMessageSizeInMiB = 512; //默认值限制为512MiB
    private int maxCachedMessageAmount = 5000; //默认值限制为5000条

    public ONSConsumerAbstract(final Properties properties) {
        super(properties);

        this.defaultMQPushConsumer = new DefaultMQPushConsumer(new OnsClientRPCHook(sessionCredentials));

        String consumerGroup = properties.getProperty(PropertyKeyConst.ConsumerId);
        if (null == consumerGroup) {
            throw new ONSClientException("ConsumerId property is null");
        }

        String maxReconsumeTimes = properties.getProperty(PropertyKeyConst.MaxReconsumeTimes);
        if (!UtilAll.isBlank(maxReconsumeTimes)) {
            try {
                this.defaultMQPushConsumer.setMaxReconsumeTimes(Integer.parseInt(maxReconsumeTimes));
            } catch (NumberFormatException ignored) {
            }
        }

        String consumeTimeout = properties.getProperty(PropertyKeyConst.ConsumeTimeout);
        if (!UtilAll.isBlank(consumeTimeout)) {
            try {
                this.defaultMQPushConsumer.setConsumeTimeout(Integer.parseInt(consumeTimeout));
            } catch (NumberFormatException ignored) {
            }
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.defaultMQPushConsumer.setVipChannelEnabled(isVipChannelEnabled);

        this.defaultMQPushConsumer.setConsumerGroup(consumerGroup);
        this.defaultMQPushConsumer.setInstanceName(this.buildIntanceName());
        this.defaultMQPushConsumer.setNamesrvAddr(this.getNameServerAddr());

        String consumeThreadNums = properties.getProperty(PropertyKeyConst.ConsumeThreadNums);
        if (!UtilAll.isBlank(consumeThreadNums)) {
            this.defaultMQPushConsumer.setConsumeThreadMin(Integer.valueOf(consumeThreadNums));
            this.defaultMQPushConsumer.setConsumeThreadMax(Integer.valueOf(consumeThreadNums));
        }

        String configuredCachedMessageAmount = properties.getProperty(PropertyKeyConst.MaxCachedMessageAmount);
        if (!UtilAll.isBlank(configuredCachedMessageAmount)) {
            maxCachedMessageAmount = Math.min(MAX_CACHED_MESSAGE_AMOUNT, Integer.valueOf(configuredCachedMessageAmount));
            maxCachedMessageAmount = Math.max(MIN_CACHED_MESSAGE_AMOUNT, maxCachedMessageAmount);
            this.defaultMQPushConsumer.setPullThresholdForTopic(maxCachedMessageAmount);

        }

        String configuredCachedMessageSizeInMiB = properties.getProperty(PropertyKeyConst.MaxCachedMessageSizeInMiB);
        if (!UtilAll.isBlank(configuredCachedMessageSizeInMiB)) {
            maxCachedMessageSizeInMiB = Math.min(MAX_CACHED_MESSAGE_SIZE_IN_MIB, Integer.valueOf(configuredCachedMessageSizeInMiB));
            maxCachedMessageSizeInMiB = Math.max(MIN_CACHED_MESSAGE_SIZE_IN_MIB, maxCachedMessageSizeInMiB);
            this.defaultMQPushConsumer.setPullThresholdSizeForTopic(maxCachedMessageSizeInMiB);
        }

        // 为Consumer增加消息轨迹回发模块
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MsgTraceSwitch);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            log.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                Properties tempProperties = new Properties();
                tempProperties.put(OnsTraceConstants.AccessKey, sessionCredentials.getAccessKey());
                tempProperties.put(OnsTraceConstants.SecretKey, sessionCredentials.getSecretKey());
                tempProperties.put(OnsTraceConstants.MaxMsgSize, "128000");
                tempProperties.put(OnsTraceConstants.AsyncBufferSize, "2048");
                tempProperties.put(OnsTraceConstants.MaxBatchNum, "100");
                tempProperties.put(OnsTraceConstants.NAMESRV_ADDR, this.getNameServerAddr());
                tempProperties.put(OnsTraceConstants.InstanceName, "PID_CLIENT_INNER_TRACE_PRODUCER");
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.CONSUMER.name());
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties);
                dispatcher.setHostConsumer(defaultMQPushConsumer.getDefaultMQPushConsumerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                    new OnsConsumeMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    void subscribe(String topic, String subExpression) {
        try {
            this.defaultMQPushConsumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            throw new ONSClientException("defaultMQPushConsumer subscribe exception", e);
        }
    }

    void subscribe(final String topic, final MessageSelector selector) {
        String subExpression = "*";
        String type = com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.TAG;
        if (selector != null) {
            if (selector.getType() == null) {
                throw new ONSClientException("Expression type is null!");
            }
            subExpression = selector.getSubExpression();
            type = selector.getType().name();
        }

        com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector messageSelector;
        if (com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.SQL92.equals(type)) {
            messageSelector = com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector.bySql(subExpression);
        } else if (com.aliyun.openservices.shade.com.alibaba.rocketmq.common.filter.ExpressionType.TAG.equals(type)) {
            messageSelector = com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.MessageSelector.byTag(subExpression);
        } else {
            throw new ONSClientException(String.format("Expression type %s is unknown!", type));
        }

        try {
            this.defaultMQPushConsumer.subscribe(topic, messageSelector);
        } catch (MQClientException e) {
            throw new ONSClientException("Consumer subscribe exception", e);
        }
    }

    void unsubscribe(String topic) {
        this.defaultMQPushConsumer.unsubscribe(topic);
    }

    @Override
    public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.defaultMQPushConsumer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.defaultMQPushConsumer.shutdown();
        }
        super.shutdown();
    }
}
