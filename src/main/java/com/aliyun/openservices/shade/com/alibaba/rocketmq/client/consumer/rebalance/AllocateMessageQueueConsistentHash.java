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
package com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consistenthash.ConsistentHashRouter;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consistenthash.HashFunction;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.consistenthash.Node;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consistent Hashing queue algorithm
 */
public class AllocateMessageQueueConsistentHash implements AllocateMessageQueueStrategy {
    private final static Logger log = LoggerFactory.getLogger("AliyunONS-client");

    private final int virtualNodeCnt;
    private final HashFunction customHashFunction;

    public AllocateMessageQueueConsistentHash() {
        this(10);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt) {
        this(virtualNodeCnt, null);
    }

    public AllocateMessageQueueConsistentHash(int virtualNodeCnt, HashFunction customHashFunction) {
        if (virtualNodeCnt < 0) {
            throw new IllegalArgumentException("illegal virtualNodeCnt :" + virtualNodeCnt);
        }
        this.virtualNodeCnt = virtualNodeCnt;
        this.customHashFunction = customHashFunction;
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        Collection<ClientNode> cidNodes = cidAll.stream().map(ClientNode::new).collect(Collectors.toList());

        final ConsistentHashRouter<ClientNode> router; //for building hash ring
        if (customHashFunction != null) {
            router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt, customHashFunction);
        } else {
            router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt);
        }

        List<MessageQueue> results = new ArrayList<>();
        mqAll.forEach(mq -> {
            ClientNode clientNode = router.routeNode(mq.toString());
            if (clientNode != null && currentCID.equals(clientNode.getKey())) {
                results.add(mq);
            }
        });

        return results;

    }

    @Override
    public String getName() {
        return "CONSISTENT_HASH";
    }

    private static class ClientNode implements Node {
        private final String clientID;

        ClientNode(String clientID) {
            this.clientID = clientID;
        }

        @Override
        public String getKey() {
            return clientID;
        }
    }

}
