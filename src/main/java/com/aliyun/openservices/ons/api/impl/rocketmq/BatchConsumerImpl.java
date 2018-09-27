package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Constants;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.aliyun.openservices.ons.api.batch.BatchConsumer;
import com.aliyun.openservices.ons.api.batch.BatchMessageListener;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class BatchConsumerImpl extends ONSConsumerAbstract implements BatchConsumer {
    private final static int MAX_BATCH_SIZE = 32;
    private final static int MIN_BATCH_SIZE = 1;
    private final ConcurrentHashMap<String/* Topic */, BatchMessageListener> subscribeTable = new ConcurrentHashMap<>();

    public BatchConsumerImpl(final Properties properties) {
        super(properties);

        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.PostSubscriptionWhenPull, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));

        String consumeBatchSize = properties.getProperty(PropertyKeyConst.ConsumeMessageBatchMaxSize);
        if (!UtilAll.isBlank(consumeBatchSize)) {
            int batchSize = Math.min(MAX_BATCH_SIZE, Integer.valueOf(consumeBatchSize));
            batchSize = Math.max(MIN_BATCH_SIZE, batchSize);
            this.defaultMQPushConsumer.setConsumeMessageBatchMaxSize(batchSize);
        }
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new BatchMessageListenerImpl());
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, BatchMessageListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, subExpression);
    }

    @Override
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }

    class BatchMessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext contextRMQ) {
            List<Message> msgList = new ArrayList<>();
            for (MessageExt rmqMsg : rmqMsgList) {
                Message msg = ONSUtil.msgConvert(rmqMsg);
                Map<String, String> propertiesMap = rmqMsg.getProperties();
                msg.setMsgID(rmqMsg.getMsgId());
                if (propertiesMap != null && propertiesMap.get(Constants.TRANSACTION_ID) != null) {
                    msg.setMsgID(propertiesMap.get(Constants.TRANSACTION_ID));
                }
                msgList.add(msg);
            }

            BatchMessageListener listener = BatchConsumerImpl.this.subscribeTable.get(msgList.get(0).getTopic());
            if (null == listener) {
                throw new ONSClientException("BatchMessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
            Action action = listener.consume(msgList, context);
            if (action != null) {
                switch (action) {
                    case CommitMessage:
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    case ReconsumeLater:
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    default:
                        break;
                }
            }
            return null;
        }
    }
}
