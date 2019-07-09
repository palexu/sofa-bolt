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
package com.alipay.remoting;

import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.RemotingUtil;
import com.alipay.remoting.util.StringUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import org.slf4j.Logger;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Log the channel status event.
 * notes
 *  Connection 事件处理器:
 *  在{@link com.alipay.remoting.rpc.RpcClient#startup()}中被注册到netty的pipline中
 *  ## 处理两类事件:
 *  1. Netty 定义的事件：
 *      例如 connect，channelActive 等
 *      在这些事件里，会根据规则触发 Reconnector 断线重连机制
 *  2. SOFABolt 定义的事件:
 *      1. {@link  ConnectionEventType#CONNECT}
 *      2. {@link  ConnectionEventType#CLOSE}
 *      3. {@link  ConnectionEventType#EXCEPTION}
 *      最终事件的发生，会触发 ConnectionEventListener 对应的逻辑
 *  ## 从功能上说：
 *  1. 触发 ConnectionEventListener
 *  2. 负责触发重连
 *  @see [SOFABolt 源码分析13 - Connection 事件处理机制的设计](https://www.jianshu.com/p/d17b60418c54)
 *
 * @author jiangping
 * @version $Id: ConnectionEventHandler.java, v 0.1 Oct 10, 2016 2:07:24 PM tao Exp $
 */
@Sharable
public class ConnectionEventHandler extends ChannelDuplexHandler {
    private static final Logger     logger = BoltLoggerFactory.getLogger("ConnectionEvent");
    /**
     * notes 这个家伙维护在这个ChannelHandler里似乎没什么用, 就是单纯的维护了一个指针
     *  倒是 {@link com.alipay.remoting.rpc.RpcConnectionEventHandler}（ConnectionEventHandler的子类）里需要用到manager去加入和删除连接
     *  所以我觉得这个私有成员变量其实可以下放到 RpcConnectionEventHandler 里
     */
    private ConnectionManager       connectionManager;

    /**
     * notes SOFABolt 定义的事件，最终在这里被处理
     */
    private ConnectionEventListener eventListener;

    /**
     * notes 就一个线程池
     */
    private ConnectionEventExecutor eventExecutor;

    private Reconnector             reconnectManager;

    private GlobalSwitch            globalSwitch;

    public ConnectionEventHandler() {

    }

    public ConnectionEventHandler(GlobalSwitch globalSwitch) {
        this.globalSwitch = globalSwitch;
    }

    /**
     * @see io.netty.channel.ChannelDuplexHandler#connect(io.netty.channel.ChannelHandlerContext, java.net.SocketAddress, java.net.SocketAddress, io.netty.channel.ChannelPromise)
     */
    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                        SocketAddress localAddress, ChannelPromise promise) throws Exception {
        if (logger.isInfoEnabled()) {
            final String local = localAddress == null ? null : RemotingUtil
                .parseSocketAddressToString(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingUtil
                .parseSocketAddressToString(remoteAddress);
            if (local == null) {
                if (logger.isInfoEnabled()) {
                    logger.info("Try connect to {}", remote);
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Try connect from {} to {}", local, remote);
                }
            }
        }
        super.connect(ctx, remoteAddress, localAddress, promise);
    }

    /**
     * @see io.netty.channel.ChannelDuplexHandler#disconnect(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
     */
    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        infoLog("Connection disconnect to {}", RemotingUtil.parseRemoteAddress(ctx.channel()));
        super.disconnect(ctx, promise);
    }

    /**
     * @see io.netty.channel.ChannelDuplexHandler#close(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
     */
    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        infoLog("Connection closed: {}", RemotingUtil.parseRemoteAddress(ctx.channel()));
        final Connection conn = ctx.channel().attr(Connection.CONNECTION).get();
        if (conn != null) {
            conn.onClose();
        }
        super.close(ctx, promise);
    }

    /**
     * @see io.netty.channel.ChannelInboundHandlerAdapter#channelRegistered(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        infoLog("Connection channel registered: {}", RemotingUtil.parseRemoteAddress(ctx.channel()));
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        infoLog("Connection channel unregistered: {}",
            RemotingUtil.parseRemoteAddress(ctx.channel()));
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        infoLog("Connection channel active: {}", RemotingUtil.parseRemoteAddress(ctx.channel()));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String remoteAddress = RemotingUtil.parseRemoteAddress(ctx.channel());
        infoLog("Connection channel inactive: {}", remoteAddress);
        super.channelInactive(ctx);
        Attribute attr = ctx.channel().attr(Connection.CONNECTION);
        if (null != attr) {
            // add reconnect task
            if (this.globalSwitch != null
                && this.globalSwitch.isOn(GlobalSwitch.CONN_RECONNECT_SWITCH)) {
                Connection conn = (Connection) attr.get();
                if (reconnectManager != null) {
                    reconnectManager.reconnect(conn.getUrl());
                }
            }
            // trigger close connection event
            onEvent((Connection) attr.get(), remoteAddress, ConnectionEventType.CLOSE);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof ConnectionEventType) {
            switch ((ConnectionEventType) event) {
                case CONNECT:
                    Channel channel = ctx.channel();
                    if (null != channel) {
                        Connection connection = channel.attr(Connection.CONNECTION).get();
                        /*
                        notes 这是干啥的？
                            > 处理bolt中定义的事件
                         */
                        this.onEvent(connection, connection.getUrl().getOriginUrl(), ConnectionEventType.CONNECT);
                    } else {
                        logger.warn("channel null when handle user triggered event in ConnectionEventHandler!");
                    }
                    break;
                default:
                    break;
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final String remoteAddress = RemotingUtil.parseRemoteAddress(ctx.channel());
        final String localAddress = RemotingUtil.parseLocalAddress(ctx.channel());
        logger
            .warn(
                "ExceptionCaught in connection: local[{}], remote[{}], close the connection! Cause[{}:{}]",
                localAddress, remoteAddress, cause.getClass().getSimpleName(), cause.getMessage());
        ctx.channel().close();
    }

    private void onEvent(final Connection conn, final String remoteAddress,
                         final ConnectionEventType type) {
        if (this.eventListener != null) {
            this.eventExecutor.onEvent(new Runnable() {
                @Override
                public void run() {
                    /*
                    notes
                     这里看起来其实不需要用 Clazz.this.eventLister 这个用法， 因为可以直接识别到eventLister是外部类的成员变量。
                     可以简写成:
                        ConnectionEventHandler.this.eventListener.onEvent(type, remoteAddress, conn);
                        ->
                        eventListener.onEvent(type, remoteAddress, conn);
                     see [Java里ClassName.this和this有什么不一样](https://segmentfault.com/q/1010000000121937)
                     */
                    ConnectionEventHandler.this.eventListener.onEvent(type, remoteAddress, conn);
                }
            });
        }
    }

    /**
     * Getter method for property <tt>listener</tt>.
     *
     * @return property value of listener
     */
    public ConnectionEventListener getConnectionEventListener() {
        return eventListener;
    }

    /**
     * Setter method for property <tt>listener</tt>.
     *
     * @param listener value to be assigned to property listener
     */
    public void setConnectionEventListener(ConnectionEventListener listener) {
        if (listener != null) {
            this.eventListener = listener;
            if (this.eventExecutor == null) {
                this.eventExecutor = new ConnectionEventExecutor();
            }
        }
    }

    /**
     * Getter method for property <tt>connectionManager</tt>.
     *
     * @return property value of connectionManager
     */
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * Setter method for property <tt>connectionManager</tt>.
     *
     * @param connectionManager value to be assigned to property connectionManager
     */
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /**
     * please use {@link ConnectionEventHandler#setReconnector(Reconnector)} instead
     * @param reconnectManager value to be assigned to property reconnectManager
     */
    @Deprecated
    public void setReconnectManager(ReconnectManager reconnectManager) {
        this.reconnectManager = reconnectManager;
    }

    public void setReconnector(Reconnector reconnector) {
        this.reconnectManager = reconnector;
    }

    /**
     * Dispatch connection event.
     *
     * @author jiangping
     * @version $Id: ConnectionEventExecutor.java, v 0.1 Mar 4, 2016 9:20:15 PM tao Exp $
     */
    public class ConnectionEventExecutor {
        Logger          logger   = BoltLoggerFactory.getLogger("CommonDefault");
        ExecutorService executor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
                                     new LinkedBlockingQueue<Runnable>(10000),
                                     new NamedThreadFactory("Bolt-conn-event-executor", true));

        /**
         * Process event.
         *
         * @param runnable Runnable
         */
        public void onEvent(Runnable runnable) {
            try {
                executor.execute(runnable);
            } catch (Throwable t) {
                logger.error("Exception caught when execute connection event!", t);
            }
        }
    }

    private void infoLog(String format, String addr) {
        if (logger.isInfoEnabled()) {
            if (StringUtils.isNotEmpty(addr)) {
                logger.info(format, addr);
            } else {
                logger.info(format, "UNKNOWN-ADDR");
            }
        }
    }
}
