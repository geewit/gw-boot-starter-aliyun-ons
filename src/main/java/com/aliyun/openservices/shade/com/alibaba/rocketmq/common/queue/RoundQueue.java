/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.shade.com.alibaba.rocketmq.common.queue;

import java.util.LinkedList;
import java.util.Queue;

/**
 * not thread safe
 */
public class RoundQueue<E> {

    private Queue<E> queue;
    private int capacity;

    public RoundQueue(int capacity) {
        this.capacity = capacity;
        queue = new LinkedList<>();
    }

    public boolean put(E e) {
        boolean ok = false;
        if (!queue.contains(e)) {
            if (queue.size() >= capacity) {
                queue.poll();
            }
            queue.add(e);
            ok = true;
        }

        return ok;
    }
}
