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
package com.alipay.remoting.connection;

import com.alipay.remoting.*;
import com.alipay.remoting.codec.Codec;
import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.config.ConfigurableInstance;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.rpc.protocol.RpcProtocol;
import com.alipay.remoting.rpc.protocol.RpcProtocolV2;
import com.alipay.remoting.util.NettyEventLoopUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * ConnectionFactory to create connection.
 *
 * @author chengyi (mark.lx@antfin.com) 2018-06-20 14:29
 */
/*
todo 这个连接工厂，虽然是abstract，但是却没有abstract方法。
 因此去掉类上的abstract标志， 其实这个类可以直接使用的。不知道为什么要作为abstract， 然后使用 DefaultConnectionFactory 来初始化。
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory {

    private static final Logger         logger      = BoltLoggerFactory
                                                        .getLogger(AbstractConnectionFactory.class);

    private static final EventLoopGroup workerGroup = NettyEventLoopUtil.newEventLoopGroup(Runtime
                                                        .getRuntime().availableProcessors() + 1,
                                                        new NamedThreadFactory(
                                                            "bolt-netty-client-worker", true));

    private final ConfigurableInstance  confInstance;
    private final Codec                 codec;
    private final ChannelHandler        heartbeatHandler;
    private final ChannelHandler        handler;

    protected Bootstrap                 bootstrap;

    public AbstractConnectionFactory(Codec codec, ChannelHandler heartbeatHandler,
                                     ChannelHandler handler, ConfigurableInstance confInstance) {
        if (codec == null) {
            throw new IllegalArgumentException("null codec");
        }
        if (handler == null) {
            throw new IllegalArgumentException("null handler");
        }

        this.confInstance = confInstance;
        this.codec = codec;
        this.heartbeatHandler = heartbeatHandler;
        this.handler = handler;
    }

    @Override
    public void init(final ConnectionEventHandler connectionEventHandler) {
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(NettyEventLoopUtil.getClientSocketChannelClass())
            .option(ChannelOption.TCP_NODELAY, ConfigManager.tcp_nodelay())
            .option(ChannelOption.SO_REUSEADDR, ConfigManager.tcp_so_reuseaddr())
            .option(ChannelOption.SO_KEEPALIVE, ConfigManager.tcp_so_keepalive());

        // init netty write buffer water mark
        initWriteBufferWaterMark();

        // init byte buf allocator
        if (ConfigManager.netty_buffer_pooled()) {
            this.bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        } else {
            this.bootstrap.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        }

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("decoder", codec.newDecoder());
                pipeline.addLast("encoder", codec.newEncoder());

                boolean idleSwitch = ConfigManager.tcp_idle_switch();
                if (idleSwitch) {
                    pipeline.addLast("idleStateHandler",
                        new IdleStateHandler(ConfigManager.tcp_idle(), ConfigManager.tcp_idle(), 0,
                            TimeUnit.MILLISECONDS));
                    pipeline.addLast("heartbeatHandler", heartbeatHandler);
                }

                pipeline.addLast("connectionEventHandler", connectionEventHandler);
                pipeline.addLast("handler", handler);
            }
        });
    }

    @Override
    public Connection createConnection(Url url) throws Exception {
        Channel channel = doCreateConnection(url.getIp(), url.getPort(), url.getConnectTimeout());
        Connection conn = new Connection(channel, ProtocolCode.fromBytes(url.getProtocol()),
            url.getVersion(), url);
        /*
        notes  这是什么操作 channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT)
            其实就是个观察者模式，发布一个用户消息后， netty就会调用对应的channelHandler里的方法
            ======see doc===========
            ChannelInboundHandler 里有下面这些方法：
            channelRegistered(..)	Channel注册到EventLoop，并且可以处理IO请求
            channelUnregistered(…)	Channel从EventLoop中被取消注册，并且不能处理任何IO请求
            channelActive(…)	    Channel已经连接到远程服务器，并准备好了接收数据
            channelInactive(…)	    Channel还没有连接到远程服务器
            channelReadComplete(…)	Channel的读取操作已经完成
            channelRead(…)	        有数据可以读取的时候触发
            [重点] userEventTriggered(…)
                当用户调用Channel.fireUserEventTriggered方法的时候，userEventTriggered就会被触发。用户可以传递一个自定义的对象当这个方法里

         */
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    @Override
    public Connection createConnection(String targetIP, int targetPort, int connectTimeout)
                                                                                           throws Exception {
        Channel channel = doCreateConnection(targetIP, targetPort, connectTimeout);
        Connection conn = new Connection(channel,
            ProtocolCode.fromBytes(RpcProtocol.PROTOCOL_CODE), RpcProtocolV2.PROTOCOL_VERSION_1,
            new Url(targetIP, targetPort));
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    @Override
    public Connection createConnection(String targetIP, int targetPort, byte version,
                                       int connectTimeout) throws Exception {
        Channel channel = doCreateConnection(targetIP, targetPort, connectTimeout);
        Connection conn = new Connection(channel,
            ProtocolCode.fromBytes(RpcProtocolV2.PROTOCOL_CODE), version, new Url(targetIP,
                targetPort));
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    /**
     * init netty write buffer water mark
     */
    private void initWriteBufferWaterMark() {
        int lowWaterMark = this.confInstance.netty_buffer_low_watermark();
        int highWaterMark = this.confInstance.netty_buffer_high_watermark();
        if (lowWaterMark > highWaterMark) {
            throw new IllegalArgumentException(
                String
                    .format(
                        "[client side] bolt netty high water mark {%s} should not be smaller than low water mark {%s} bytes)",
                        highWaterMark, lowWaterMark));
        } else {
            logger.warn(
                "[client side] bolt netty low water mark is {} bytes, high water mark is {} bytes",
                lowWaterMark, highWaterMark);
        }
        /*
        notes ChannelOption.WRITE_BUFFER_WATER_MARK 这个配置项的作用是什么
            > see https://juejin.im/post/5c6f5b04f265da2dd218cb03
            > 通过 WRITE_BUFFER_WATER_MARK 设置某个连接上可以暂存的最大最小 Buffer 之后，
            如果该连接的等待发送的数据量大于设置的值时，则 isWritable 会返回不可写。这样，客户端可以不再发送，防止这个量不断的积压，最终可能让客户端挂掉。
            如果发生这种情况，一般是服务端处理缓慢导致。这个值可以有效的保护客户端。此时数据并没有发送出去。
         */
        this.bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
            lowWaterMark, highWaterMark));
    }

    /*
    bootstrap 是可以复用的， sofa使用同一个bootstrap重复connect去连接多个机器端口
     */
    @SuppressWarnings("AlibabaRemoveCommentedCode")
    protected Channel doCreateConnection(String targetIP, int targetPort, int connectTimeout)
                                                                                             throws Exception {
        // prevent unreasonable value, at least 1000
        connectTimeout = Math.max(connectTimeout, 1000);
        String address = targetIP + ":" + targetPort;
        if (logger.isDebugEnabled()) {
            logger.debug("connectTimeout of address [{}] is [{}].", address, connectTimeout);
        }
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(targetIP, targetPort));

        /*
        1. 这个什么作用？
        > 等待future 完成，不可打断. 这个是阻塞的

        2. todo 为什么在客户端创建到服务端的连接时，需要阻塞型呢？
        >


        资料see https://www.cnblogs.com/sorheart/p/3195099.html

        源码 io.netty.util.concurrent.DefaultPromise.awaitUninterruptibly()
        ```java
        if (this.isDone()) {
            return this;
        } else {
            this.checkDeadLock();
            boolean interrupted = false;
            synchronized(this) {
                while(!this.isDone()) {
                    this.incWaiters();

                    try {
                        this.wait();
                    } catch (InterruptedException var9) {
                        interrupted = true;
                    } finally {
                        this.decWaiters();
                    }
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            return this;
        }
        ```
         */
        future.awaitUninterruptibly();
        if (!future.isDone()) {
            String errMsg = "Create connection to " + address + " timeout!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (future.isCancelled()) {
            String errMsg = "Create connection to " + address + " cancelled by user!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (!future.isSuccess()) {
            String errMsg = "Create connection to " + address + " error!";
            logger.warn(errMsg);
            throw new Exception(errMsg, future.cause());
        }
        return future.channel();
    }
}
