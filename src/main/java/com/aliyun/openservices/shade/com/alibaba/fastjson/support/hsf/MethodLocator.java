package com.aliyun.openservices.shade.com.alibaba.fastjson.support.hsf;

import java.lang.reflect.Method;

public interface MethodLocator {
    Method findMethod(String[] types);
}
