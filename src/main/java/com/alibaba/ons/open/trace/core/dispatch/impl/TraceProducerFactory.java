package com.alibaba.ons.open.trace.core.dispatch.impl;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.namesrv.TopAddressing;

import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;
import com.aliyun.openservices.ons.api.impl.rocketmq.ClientRPCHook;

/**
 * @author MQDevelopers
 */
public class TraceProducerFactory {
    private static Map<String, Object> dispatcherTable = new ConcurrentHashMap<String, Object>();
    private static AtomicBoolean isStarted = new AtomicBoolean(false);
    private static DefaultMQProducer traceProducer;

    public static DefaultMQProducer getTraceDispatcherProducer(Properties properties) {
        if (traceProducer == null) {
            SessionCredentials sessionCredentials = new SessionCredentials();
            Properties sessionProperties = new Properties();
            String accessKey = properties.getProperty(OnsTraceConstants.AccessKey);
            String secretKey = properties.getProperty(OnsTraceConstants.SecretKey);
            sessionProperties.put(OnsTraceConstants.AccessKey, accessKey);
            sessionProperties.put(OnsTraceConstants.SecretKey, secretKey);
            sessionCredentials.updateContent(sessionProperties);
            traceProducer = new DefaultMQProducer(new ClientRPCHook(sessionCredentials));
            traceProducer.setProducerGroup(accessKey + OnsTraceConstants.groupName);
            traceProducer.setSendMsgTimeout(5000);
            traceProducer.setInstanceName(properties.getProperty(OnsTraceConstants.InstanceName, String.valueOf(System.currentTimeMillis())));
            String nameSrv = properties.getProperty(OnsTraceConstants.NAMESRV_ADDR);
            if (nameSrv == null) {
                TopAddressing topAddressing = new TopAddressing(properties.getProperty(OnsTraceConstants.ADDRSRV_URL));
                nameSrv = topAddressing.fetchNSAddr();
            }
            traceProducer.setNamesrvAddr(nameSrv);
            traceProducer.setVipChannelEnabled(false);
            // 消息最大大小128K
            int maxSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxMsgSize, "128000"));
            traceProducer.setMaxMessageSize(maxSize - 10 * 1000);
        }
        return traceProducer;
    }

    public static DefaultMQProducer getTraceDispatcherProducer(Properties properties, SessionCredentials sessionCredentials) {
        if (traceProducer == null) {
            String accessKey = properties.getProperty(OnsTraceConstants.AccessKey);
            traceProducer = new DefaultMQProducer(new ClientRPCHook(sessionCredentials));
            traceProducer.setProducerGroup(accessKey.replace('.', '-') + OnsTraceConstants.groupName);
            traceProducer.setSendMsgTimeout(5000);
            traceProducer.setInstanceName(properties.getProperty(OnsTraceConstants.InstanceName, String.valueOf(System.currentTimeMillis())));
            String nameSrv = properties.getProperty(OnsTraceConstants.NAMESRV_ADDR);
            if (nameSrv == null) {
                TopAddressing topAddressing = new TopAddressing(properties.getProperty(OnsTraceConstants.ADDRSRV_URL));
                nameSrv = topAddressing.fetchNSAddr();
            }
            traceProducer.setNamesrvAddr(nameSrv);
            traceProducer.setVipChannelEnabled(false);
            // 消息最大大小128K
            int maxSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxMsgSize, "128000"));
            traceProducer.setMaxMessageSize(maxSize - 10 * 1000);
        }
        return traceProducer;
    }

    public static void registerTraceDispatcher(String dispatcherId) throws MQClientException {
        dispatcherTable.put(dispatcherId, new Object());
        if (traceProducer != null && isStarted.compareAndSet(false, true)) {
            traceProducer.start();
        }
    }

    public static void unregisterTraceDispatcher(String dispatcherId) {
        dispatcherTable.remove(dispatcherId);
        if (dispatcherTable.isEmpty() && traceProducer != null && isStarted.get()) {
            traceProducer.shutdown();
        }
    }
}
