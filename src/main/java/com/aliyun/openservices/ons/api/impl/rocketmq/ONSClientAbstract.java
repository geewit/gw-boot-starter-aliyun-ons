package com.aliyun.openservices.ons.api.impl.rocketmq;

import com.alibaba.ons.open.trace.core.dispatch.AsyncDispatcher;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.impl.authority.SessionCredentials;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.openservices.shade.com.alibaba.rocketmq.common.UtilAll.getPid;

public abstract class ONSClientAbstract implements Admin {
    // 内网地址服务器
    protected static final String WSADDR_INTERNAL = System.getProperty("com.aliyun.openservices.ons.addr.internal",
        "http://onsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal");
    // 公网地址服务器
    protected static final String WSADDR_INTERNET = System.getProperty("com.aliyun.openservices.ons.addr.internet",
        "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet");
    protected static final long WSADDR_INTERNAL_TIMEOUTMILLS =
        Long.parseLong(System.getProperty("com.aliyun.openservices.ons.addr.internal.timeoutmills", "3000"));
    protected static final long WSADDR_INTERNET_TIMEOUTMILLS =
            Long.parseLong(System.getProperty("com.aliyun.openservices.ons.addr.internet.timeoutmills", "5000"));
    private final static Logger log = LoggerFactory.getLogger("AliyunONS-client");
    protected final Properties properties;
    protected final SessionCredentials sessionCredentials = new SessionCredentials();
    protected String nameServerAddr;

    protected AsyncDispatcher traceDispatcher = null;

    protected final AtomicBoolean started = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ONSClient-UpdateNameServerThread"));

    public ONSClientAbstract(Properties properties) {
        this.properties = properties;
        this.sessionCredentials.updateContent(properties);
        // 检测必须的参数
        if (StringUtils.isEmpty(this.sessionCredentials.getAccessKey())) {
            throw new ONSClientException("please set access key");
        }

        if (StringUtils.isEmpty(this.sessionCredentials.getSecretKey())) {
            throw new ONSClientException("please set secret key");
        }

        if (null == this.sessionCredentials.getOnsChannel()) {
            throw new ONSClientException("please set ons channel");
        }

        // 用户指定了Name Server
        // 私有云模式有可能需要
        this.nameServerAddr = this.properties.getProperty(PropertyKeyConst.NAMESRV_ADDR);
        if (nameServerAddr == null) {
            this.nameServerAddr = fetchNameServerAddr();
            if (null == nameServerAddr) {
                throw new ONSClientException(FAQ.errorMessage("Can not find name server, May be your network problem.", FAQ.FIND_NS_FAILED));
            }

            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    String nsAddrs = fetchNameServerAddr();
                    if (nsAddrs != null && !ONSClientAbstract.this.nameServerAddr.equals(nsAddrs)) {
                        ONSClientAbstract.this.nameServerAddr = nsAddrs;
                        if (isStarted()) {
                            updateNameServerAddr(nsAddrs);
                        }
                    }
                } catch (Exception e) {
                    log.error("update name server periodically failed.", e);
                }
            }, 10 * 1000L, 30 * 1000L, TimeUnit.MILLISECONDS);
        }
    }

    protected abstract void updateNameServerAddr(String newAddrs);

    private String fetchNameServerAddr() {
        String nsAddrs;

        // 用户指定了地址服务器
        {
            String property = this.properties.getProperty(PropertyKeyConst.ONSAddr);
            log.info("onsAddr = " + property);
            if (property != null) {
                nsAddrs = new TopAddressing(property).fetchNSAddr();
                if (nsAddrs != null) {
                    log.info("connected to user-defined ons addr server, {} success, {}", property, nsAddrs);
                    return nsAddrs;
                } else {
                    throw new ONSClientException(FAQ.errorMessage("Can not find name server with onsAddr " + property, FAQ.FIND_NS_FAILED));
                }
            }
        }

        // 用户未指定，默认访问内网地址服务器
        {
            TopAddressing top = new TopAddressing(WSADDR_INTERNAL);
            nsAddrs = top.fetchNSAddr(false, WSADDR_INTERNAL_TIMEOUTMILLS);
            if (nsAddrs != null) {
                log.info("connected to internal server, {} success, {}", WSADDR_INTERNAL, nsAddrs);
                return nsAddrs;
            }
        }

        // 用户未指定，然后访问公网地址服务器
        {
            TopAddressing top = new TopAddressing(WSADDR_INTERNET);
            nsAddrs = top.fetchNSAddr(false, WSADDR_INTERNET_TIMEOUTMILLS);
            if (nsAddrs != null) {
                log.info("connected to internet server, {} success, {}", WSADDR_INTERNET, nsAddrs);
            }
        }

        return nsAddrs;
    }

    public String getNameServerAddr() {
        return this.nameServerAddr;
    }

    protected String buildIntanceName() {
        return Integer.toString(UtilAll.getPid())//
            + "#" + this.nameServerAddr.hashCode()//
            + "#" + this.sessionCredentials.getAccessKey().hashCode()
            + "#" + System.nanoTime();
    }

    protected void checkONSProducerServiceState(DefaultMQProducerImpl producer) {
        switch (producer.getServiceState()) {
            case CREATE_JUST:
                throw new ONSClientException(
                    FAQ.errorMessage(String.format("You do not have start the producer[" + getPid() + "], %s", producer.getServiceState()),
                        FAQ.SERVICE_STATE_WRONG));
            case SHUTDOWN_ALREADY:
                throw new ONSClientException(FAQ.errorMessage(String.format("Your producer has been shut down, %s", producer.getServiceState()),
                    FAQ.SERVICE_STATE_WRONG));
            case START_FAILED:
                throw new ONSClientException(FAQ.errorMessage(
                    String.format("When you start your service throws an exception, %s", producer.getServiceState()), FAQ.SERVICE_STATE_WRONG));
            case RUNNING:
                break;
            default:
                break;
        }
    }

    @Override
    public void start() {
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start();
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
        scheduledExecutorService.shutdown();
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public boolean isClosed() {
        return !isStarted();
    }
}
