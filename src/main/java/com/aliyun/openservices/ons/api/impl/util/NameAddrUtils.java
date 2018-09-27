package com.aliyun.openservices.ons.api.impl.util;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.MixAll;

/**
 * User: yubao.fyb
 * Date: 14/12/15
 * Time: 11:31
 */
public class NameAddrUtils {
    public static String getNameAdd() {
        return System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
                System.getenv(MixAll.NAMESRV_ADDR_ENV));
    }
}
