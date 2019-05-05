package com.aliyun.openservices.ons.api.impl.rocketmq;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Generated;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Constants;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.MessageSelector;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.PropertyValueConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;

@Generated("ons-client")
public class ConsumerImpl extends ONSConsumerAbstract implements Consumer {
    private final ConcurrentHashMap<String, MessageListener> subscribeTable = new ConcurrentHashMap<String, MessageListener>();

    public ConsumerImpl(final Properties properties) {
        super(properties);
        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.PostSubscriptionWhenPull, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MessageModel, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerImpl());
        super.start();
    }


    @Override
    public void subscribe(String topic, String subExpression, MessageListener listener) {
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
    public void subscribe(final String topic, final MessageSelector selector, final MessageListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }


    @Override
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }


    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgsRMQList,
            ConsumeConcurrentlyContext contextRMQ) {
            MessageExt msgRMQ = msgsRMQList.get(0);
            Message msg = ONSUtil.msgConvert(msgRMQ);
            Map<String, String> stringStringMap = msgRMQ.getProperties();
            msg.setMsgID(msgRMQ.getMsgId());
            if (stringStringMap != null && stringStringMap.get(Constants.TRANSACTION_ID) != null) {
                msg.setMsgID(stringStringMap.get(Constants.TRANSACTION_ID));
            }
            MessageListener listener = ConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new ONSClientException("MessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
            Action action = listener.consume(msg, context);
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
