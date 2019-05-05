/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.aliyun.openservices.shade.io.netty.example.worldclock;

import com.aliyun.openservices.shade.io.netty.bootstrap.ServerBootstrap;
import com.aliyun.openservices.shade.io.netty.channel.EventLoopGroup;
import com.aliyun.openservices.shade.io.netty.channel.nio.NioEventLoopGroup;
import com.aliyun.openservices.shade.io.netty.channel.socket.nio.NioServerSocketChannel;
import com.aliyun.openservices.shade.io.netty.handler.logging.LogLevel;
import com.aliyun.openservices.shade.io.netty.handler.logging.LoggingHandler;
import com.aliyun.openservices.shade.io.netty.handler.ssl.SslContext;
import com.aliyun.openservices.shade.io.netty.handler.ssl.SslContextBuilder;
import com.aliyun.openservices.shade.io.netty.handler.ssl.util.SelfSignedCertificate;

/**
 * Receives a list of continent/city pairs from a {@link WorldClockClient} to
 * get the local times of the specified cities.
 */
public final class WorldClockServer {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final int PORT = Integer.parseInt(System.getProperty("port", "8463"));

    public static void main(String[] args) throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new WorldClockServerInitializer(sslCtx));

            b.bind(PORT).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}