/*
 * Copyright 2014 The Netty Project
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
package com.aliyun.openservices.shade.io.netty.example.spdy.client;

import com.aliyun.openservices.shade.io.netty.bootstrap.Bootstrap;
import com.aliyun.openservices.shade.io.netty.channel.Channel;
import com.aliyun.openservices.shade.io.netty.channel.ChannelOption;
import com.aliyun.openservices.shade.io.netty.channel.EventLoopGroup;
import com.aliyun.openservices.shade.io.netty.channel.nio.NioEventLoopGroup;
import com.aliyun.openservices.shade.io.netty.channel.socket.nio.NioSocketChannel;
import com.aliyun.openservices.shade.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.aliyun.openservices.shade.io.netty.handler.codec.http.HttpHeaders;
import com.aliyun.openservices.shade.io.netty.handler.codec.http.HttpMethod;
import com.aliyun.openservices.shade.io.netty.handler.codec.http.HttpRequest;
import com.aliyun.openservices.shade.io.netty.handler.codec.http.HttpVersion;
import com.aliyun.openservices.shade.io.netty.handler.ssl.ApplicationProtocolConfig;
import com.aliyun.openservices.shade.io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import com.aliyun.openservices.shade.io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import com.aliyun.openservices.shade.io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import com.aliyun.openservices.shade.io.netty.handler.ssl.ApplicationProtocolNames;
import com.aliyun.openservices.shade.io.netty.handler.ssl.SslContext;
import com.aliyun.openservices.shade.io.netty.handler.ssl.SslContextBuilder;
import com.aliyun.openservices.shade.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * An SPDY client that allows you to send HTTP GET to a SPDY server.
 * <p>
 * This class must be run with the JVM parameter: {@code java -Xbootclasspath/p:<path_to_npn_boot_jar> ...}. The
 * "path_to_npn_boot_jar" is the path on the file system for the NPN Boot Jar file which can be downloaded from Maven at
 * coordinates org.mortbay.jetty.npn:npn-boot. Different versions applies to different OpenJDK versions. See
 * <a href="http://www.eclipse.org/jetty/documentation/current/npn-chapter.html">Jetty docs</a> for more information.
 * <p>
 * You may also use the {@code run-example.sh} script to start the client from the command line:
 * <pre>
 *     ./run-example.sh spdy-client
 * </pre>
 */
public final class SpdyClient {

    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8443"));

    public static void main(String[] args) throws Exception {
        // Configure SSL.
        final SslContext sslCtx = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocolConfig(new ApplicationProtocolConfig(
                        Protocol.NPN,
                        // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
                        SelectorFailureBehavior.NO_ADVERTISE,
                        // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
                        SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.SPDY_3_1,
                        ApplicationProtocolNames.HTTP_1_1))
            .build();

        HttpResponseClientHandler httpResponseHandler = new HttpResponseClientHandler();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.remoteAddress(HOST, PORT);
            b.handler(new SpdyClientInitializer(sslCtx, httpResponseHandler));

            // Start the client.
            Channel channel = b.connect().syncUninterruptibly().channel();
            System.out.println("Connected to " + HOST + ':' + PORT);

            // Create a GET request.
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "");
            request.headers().set(HttpHeaders.Names.HOST, HOST);
            request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

            // Send the GET request.
            channel.writeAndFlush(request).sync();

            // Waits for the complete HTTP response
            httpResponseHandler.queue().take().sync();
            System.out.println("Finished SPDY HTTP GET");

            // Wait until the connection is closed.
            channel.close().syncUninterruptibly();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
