package com.aliyun.openservices.ons.api.impl.rocketmq;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageExt;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageSelector;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.order.ConsumeOrderContext;
import com.aliyun.openservices.ons.api.order.MessageOrderListener;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;

public class OrderConsumerImpl extends ONSConsumerAbstract implements OrderConsumer {
    private final ConcurrentHashMap<String, MessageOrderListener> subscribeTable = new ConcurrentHashMap<String, MessageOrderListener>();

    public OrderConsumerImpl(final Properties properties) {
        super(properties);
        String suspendTimeMillis = properties.getProperty(PropertyKeyConst.SuspendTimeMillis);
        if (!UtilAll.isBlank(suspendTimeMillis)) {
            try {
                this.defaultMQPushConsumer.setSuspendCurrentQueueTimeMillis(Long.parseLong(suspendTimeMillis));
            } catch (NumberFormatException ignored) {
            }
        }
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderlyImpl());
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, MessageOrderListener listener) {
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
    public void subscribe(final String topic, final MessageSelector selector, final MessageOrderListener listener) {
        if (null == topic) {
            throw new ONSClientException("topic is null");
        }

        if (null == listener) {
            throw new ONSClientException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }

    class MessageListenerOrderlyImpl implements MessageListenerOrderly {

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> arg0, ConsumeOrderlyContext arg1) {
            MessageExt msgRMQ = arg0.get(0);
            Message msg = ONSUtil.msgConvert(msgRMQ);
            msg.setMsgID(msgRMQ.getMsgId());

            MessageOrderListener listener = OrderConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new ONSClientException("MessageOrderListener is null");
            }

            final ConsumeOrderContext context = new ConsumeOrderContext();
            OrderAction action = listener.consume(msg, context);
            if (action != null) {
                switch (action) {
                    case Success:
                        return ConsumeOrderlyStatus.SUCCESS;
                    case Suspend:
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    default:
                        break;
                }
            }

            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
