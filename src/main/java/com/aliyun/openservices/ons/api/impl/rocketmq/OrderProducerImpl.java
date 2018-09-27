package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.tracehook.OnsClientSendMessageHookImpl;
import com.aliyun.openservices.ons.api.order.OrderProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class OrderProducerImpl extends ONSClientAbstract implements OrderProducer {
    private final static Logger log = LoggerFactory.getLogger("AliyunONS-client");
    private final DefaultMQProducer defaultMQProducer;

    public OrderProducerImpl(final Properties properties) {
        super(properties);
        this.defaultMQProducer = new DefaultMQProducer(new OnsClientRPCHook(sessionCredentials));

        String producerGroup = properties.getProperty(PropertyKeyConst.ProducerId, "__ONS_PRODUCER_DEFAULT_GROUP");
        this.defaultMQProducer.setProducerGroup(producerGroup);

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.defaultMQProducer.setVipChannelEnabled(isVipChannelEnabled);

        String sendMsgTimeoutMillis = properties.getProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        this.defaultMQProducer.setSendMsgTimeout(Integer.parseInt(sendMsgTimeoutMillis));

        this.defaultMQProducer.setInstanceName(this.buildIntanceName());
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
        // 为Producer增加消息轨迹回发模块
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
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.PRODUCER.name());
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties);
                dispatcher.setHostProducer(defaultMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void start() {
        try {
            if (started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final String shardingKey) {
        if (UtilAll.isBlank(shardingKey)) {
            throw new ONSClientException("\'shardingKey\' is blank.");
        }
        message.setShardingKey(shardingKey);
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        final com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        try {
            com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult sendResultRMQ =
                this.defaultMQProducer.send(msgRMQ, (mqs, msg, shardingKey1) -> {
                    int select = Math.abs(shardingKey1.hashCode());
                    if (select < 0) {
                        select = 0;
                    }
                    return mqs.get(select % mqs.size());
                }, shardingKey);
            message.setMsgID(sendResultRMQ.getMsgId());
            SendResult sendResult = new SendResult();
            sendResult.setTopic(message.getTopic());
            sendResult.setMessageId(sendResultRMQ.getMsgId());
            return sendResult;
        } catch (Exception e) {
            throw new ONSClientException("defaultMQProducer send order exception", e);
        }
    }
}
