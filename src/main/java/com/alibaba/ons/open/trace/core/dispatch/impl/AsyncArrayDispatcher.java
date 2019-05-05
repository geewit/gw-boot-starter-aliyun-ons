package com.alibaba.ons.open.trace.core.dispatch.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.ons.open.trace.core.common.OnsTraceConstants;
import com.alibaba.ons.open.trace.core.common.OnsTraceContext;
import com.alibaba.ons.open.trace.core.common.OnsTraceDataEncoder;
import com.alibaba.ons.open.trace.core.common.OnsTraceDispatcherType;
import com.alibaba.ons.open.trace.core.common.OnsTraceTransferBean;
import com.alibaba.ons.open.trace.core.dispatch.AsyncDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.common.ThreadLocalIndex;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.log.ClientLogger;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendCallback;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.NamespaceUtil;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.logging.InternalLogger;

import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;

/**
 * @author MQDevelopers
 */
public class AsyncArrayDispatcher implements AsyncDispatcher {
    private final static InternalLogger CLIENT_LOG = ClientLogger.getLog();
    private final int queueSize;
    private final int batchSize;
    private final DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecuter;
    /**
     * 最近丢弃的日志条数
     */
    private AtomicLong discardCount;
    private Thread worker;
    private ArrayBlockingQueue<OnsTraceContext> traceContextQueue;
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private String dispatcherType;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private String dispatcherId = UUID.randomUUID().toString();

    public AsyncArrayDispatcher(Properties properties) throws MQClientException {
        dispatcherType = properties.getProperty(OnsTraceConstants.TraceDispatcherType);
        int queueSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.AsyncBufferSize, "2048"));
        // queueSize 取大于或等于 value 的 2 的 n 次方数
        queueSize = 1 << (32 - Integer.numberOfLeadingZeros(queueSize - 1));
        this.queueSize = queueSize;
        batchSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxBatchNum, "1"));
        this.discardCount = new AtomicLong(0L);
        traceContextQueue = new ArrayBlockingQueue<OnsTraceContext>(1024);
        appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);

        this.traceExecuter = new ThreadPoolExecutor(//
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = TraceProducerFactory.getTraceDispatcherProducer(properties);
    }

    public AsyncArrayDispatcher(Properties properties, SessionCredentials sessionCredentials) throws MQClientException {
        dispatcherType = properties.getProperty(OnsTraceConstants.TraceDispatcherType);
        int queueSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.AsyncBufferSize, "2048"));
        // queueSize 取大于或等于 value 的 2 的 n 次方数
        queueSize = 1 << (32 - Integer.numberOfLeadingZeros(queueSize - 1));
        this.queueSize = queueSize;
        batchSize = Integer.parseInt(properties.getProperty(OnsTraceConstants.MaxBatchNum, "1"));
        this.discardCount = new AtomicLong(0L);
        traceContextQueue = new ArrayBlockingQueue<OnsTraceContext>(1024);
        appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);

        this.traceExecuter = new ThreadPoolExecutor(
            10,
            20,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.appenderQueue,
            new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = TraceProducerFactory.getTraceDispatcherProducer(properties, sessionCredentials);
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    @Override
    public void start() throws MQClientException {
        TraceProducerFactory.registerTraceDispatcher(dispatcherId);
        this.worker = new ThreadFactoryImpl("MQ-AsyncArrayDispatcher-Thread-" + dispatcherId, true)
            .newThread(new AsyncRunnable());
        this.worker.start();
        this.registerShutDownHook();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((OnsTraceContext) ctx);
        if (!result) {
            CLIENT_LOG.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        // 最多等待刷新的时间，避免数据一直在写导致无法返回
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        CLIENT_LOG.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.traceExecuter.shutdown();
        TraceProducerFactory.unregisterTraceDispatcher(dispatcherId);
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new ThreadFactoryImpl("ShutdownHookMQTrace").newThread(new Runnable() {
                private volatile boolean hasShutdown = false;
                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            try {
                                flush();
                            } catch (IOException e) {
                                CLIENT_LOG.error("system mqtrace hook shutdown failed ,maybe loss some trace data");
                            }
                        }
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutDownHook);
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                List<OnsTraceContext> contexts = new ArrayList<OnsTraceContext>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    OnsTraceContext context = null;
                    try {
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }
                if (contexts.size() > 0) {
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecuter.submit(request);
                } else if (AsyncArrayDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<OnsTraceContext> contextList;

        public AsyncAppenderRequest(final List<OnsTraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<OnsTraceContext>(1);
            }
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }

        /**
         * 往消息缓冲区编码轨迹数据
         *
         * @param contextList
         */
        public void sendTraceData(List<OnsTraceContext> contextList) {
            Map<String, List<OnsTraceTransferBean>> transBeanMap = new HashMap<String, List<OnsTraceTransferBean>>(16);
            String currentRegionId = null;
            for (OnsTraceContext context : contextList) {
                currentRegionId = context.getRegionId();
                if (currentRegionId == null || context.getTraceBeans().isEmpty()) {
                    continue;
                }
                String topic = context.getTraceBeans().get(0).getTopic();
                String key = topic + OnsTraceConstants.CONTENT_SPLITOR + currentRegionId;
                List<OnsTraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<OnsTraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                OnsTraceTransferBean traceData = OnsTraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<OnsTraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(OnsTraceConstants.CONTENT_SPLITOR));
                flushData(entry.getValue(), key[0], key[1]);
            }
        }

        /**
         * 实际批量发送数据
         */
        private void flushData(List<OnsTraceTransferBean> transBeanList, String topic, String currentRegionId) {
            if (transBeanList.size() == 0) {
                return;
            }
            // 临时缓冲区
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();

            for (OnsTraceTransferBean bean : transBeanList) {
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // 保证包的大小不要超过上限
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId);
                    // 发送完成，清除临时缓冲区
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), topic, currentRegionId);
            }
            transBeanList.clear();
        }

        /**
         * 发送数据的接口
         *
         * @param keySet 本批次包含的keyset
         * @param data 本批次的轨迹数据
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data, String dataTopic,
            String currentRegionId) {
            String topic = OnsTraceConstants.traceTopic + currentRegionId;
            final Message message = new Message(topic, data.getBytes());
            message.setKeys(keySet);
            try {
                Set<String> dataBrokerSet = getBrokerSetByTopic(dataTopic);
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), topic);
                dataBrokerSet.retainAll(traceBrokerSet);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                    }

                    @Override
                    public void onException(Throwable e) {
                        //todo 对于发送失败的数据，如何保存，保证所有轨迹数据都记录下来
                        CLIENT_LOG.info("send trace data ,the traceData is " + data);
                    }
                };
                if (dataBrokerSet.isEmpty()) {
                    //no cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.getAndIncrement();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, dataBrokerSet, callback);
                }

            } catch (Exception e) {
                CLIENT_LOG.info("send trace data,the traceData is" + data);
            }
        }

        private Set<String> getBrokerSetByTopic(String topic) {
            Set<String> brokerSet = new HashSet<String>();
            if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.PRODUCER.name()) && hostProducer != null) {
                brokerSet = tryGetMessageQueueBrokerSet(hostProducer, topic);
            }
            if (dispatcherType != null && dispatcherType.equals(OnsTraceDispatcherType.CONSUMER.name()) && hostConsumer != null) {
                brokerSet = tryGetMessageQueueBrokerSet(hostConsumer, topic);
            }
            return brokerSet;
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            String realTopic = NamespaceUtil.wrapNamespace(producer.getDefaultMQProducer().getNamespace(), topic);
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(realTopic, new TopicPublishInfo());
                producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(realTopic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(realTopic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQPushConsumerImpl consumer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            try {
                String realTopic = NamespaceUtil.wrapNamespace(consumer.getDefaultMQPushConsumer().getNamespace(), topic);
                Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(realTopic);
                for (MessageQueue queue : messageQueues) {
                    brokerSet.add(queue.getBrokerName());
                }
            } catch (MQClientException e) {
                CLIENT_LOG.info("fetch message queue failed, the topic is {}", topic);
            }
            return brokerSet;
        }
    }
}
