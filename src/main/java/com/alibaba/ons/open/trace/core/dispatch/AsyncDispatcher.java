package com.alibaba.ons.open.trace.core.dispatch;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import java.io.IOException;

/**
 * 异步传输数据模块
 */
public interface AsyncDispatcher {
    /**
     * 初始化异步传输数据模块
     */
    void start() throws MQClientException;

    /**
     * 往数据传输模块中添加信息，
     *
     * @param ctx 数据信息
     * @return 返回true代表添加成功，返回false代表数据被抛弃
     */
    boolean append(Object ctx);

    /**
     * 强制调用写操作
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * 关闭轨迹Hook
     */
    void shutdown();
}
