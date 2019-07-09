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
package com.alipay.remoting.rpc;

import com.alipay.remoting.*;
import com.alipay.remoting.config.BoltGenericOption;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.alipay.remoting.rpc.protocol.UserProcessorRegisterHelper;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client for Rpc.
 *
 * @author jiangping
 * @version $Id: RpcClient.java, v 0.1 2015-9-23 PM4:03:28 tao Exp $
 */
public class RpcClient extends AbstractBoltClient {

    private static final Logger                               logger = BoltLoggerFactory
                                                                         .getLogger("RpcRemoting");

    private final RpcTaskScanner                              taskScanner;
    /**
     * notes RpcClient不是客户端么？为什么也需要UserProcessor来处理请求？
     *  RpcServer 可以主动向 RpcClient 发起请求，所以RpcClient也需要创建{@link UserProcessor}来处理这些请求
     *  通过{@link #registerUserProcessor}向RpcClient注册
     */
    private final ConcurrentHashMap<String, UserProcessor<?>> userProcessors;
    private final ConnectionEventHandler                      connectionEventHandler;
    private final ConnectionEventListener                     connectionEventListener;

    private DefaultClientConnectionManager                    connectionManager;
    private Reconnector                                       reconnectManager;
    private RemotingAddressParser                             addressParser;
    private DefaultConnectionMonitor                          connectionMonitor;
    private ConnectionMonitorStrategy                         monitorStrategy;

    // used in RpcClientAdapter (bolt-tr-adapter)
    protected RpcRemoting                                     rpcRemoting;

    public RpcClient() {
        this.taskScanner = new RpcTaskScanner();
        this.userProcessors = new ConcurrentHashMap<String, UserProcessor<?>>();
        this.connectionEventHandler = new RpcConnectionEventHandler(switches());
        this.connectionEventListener = new ConnectionEventListener();
    }

    /**
     * Please use {@link RpcClient#startup()} instead
     */
    @Deprecated
    public void init() {
        startup();
    }

    /**
     * Shutdown.
     * <p>
     * Notice:<br>
     *   <li>Rpc client can not be used any more after shutdown.
     *   <li>If you need, you should destroy it, and instantiate another one.
     */
    @Override
    public void shutdown() {
        super.shutdown();

        this.connectionManager.shutdown();
        logger.warn("Close all connections from client side!");
        this.taskScanner.shutdown();
        logger.warn("Rpc client shutdown!");
        if (reconnectManager != null) {
            reconnectManager.shutdown();
        }
        if (connectionMonitor != null) {
            connectionMonitor.shutdown();
        }
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        if (this.addressParser == null) {
            this.addressParser = new RpcAddressParser();
        }

        ConnectionSelectStrategy connectionSelectStrategy = option(BoltGenericOption.CONNECTION_SELECT_STRATEGY);
        if (connectionSelectStrategy == null) {
            connectionSelectStrategy = new RandomSelectStrategy(switches());
        }

        /**
         * 第一步
       notes 配置并启动 DefaultClientConnectionManager
        此时， {@link #userProcessors} 应当已经配置好了用户的处理器。
        当然没有配置也没关系，后面也可以注册进去。因为提供了 register方法
        一共有以下handler:
        1. 需要进行配置才可以使用的：
            1) {@link RpcHandler} 内部包装了所有的userProcessors,所有用户自定义的消息处理逻辑都在这里注册和触发.
                为什么要怎么做呢？可以参考 see ['Netty 入门与实战：仿写微信 IM 即时通讯系统'里的骚操作]https://www.jianshu.com/p/bb0805d65388
                > 缩短事件传播路径—— 放 Map 里，在第一个 handler 里根据指令来找具体 handler。
                > RpcHandler就是利用 UserProcessor 感兴趣的时间作为map的key， 来缩短事件传播路径。
            2) {@link ConnectionEventHandler} 处理所有sofa-bolt源代码里定义的一些事件，用户不需要关心这个handler。
        2. 无需配置即可使用的
            1) 编解码器handler
            2) heartbeatHandler
        =================
        see {@link com.alipay.remoting.connection.AbstractConnectionFactory#init}
        ```
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("decoder", codec.newDecoder());
                pipeline.addLast("encoder", codec.newEncoder());
                //
                boolean idleSwitch = ConfigManager.tcp_idle_switch();
                if (idleSwitch) {
                    pipeline.addLast("idleStateHandler",
                        new IdleStateHandler(ConfigManager.tcp_idle(), ConfigManager.tcp_idle(), 0,
                            TimeUnit.MILLISECONDS));
                    pipeline.addLast("heartbeatHandler", heartbeatHandler);
                }
                //
                pipeline.addLast("connectionEventHandler", connectionEventHandler);
                pipeline.addLast("handler", handler);
            }
        });

         todo 所以 {@link DefaultConnectionManager} 有什么功能呢？
          1. 提供同步、异步、单向调用等方式 （这里其实是代理了 RpcRemoting 的方法)
          2. 调用时会按需创建对应channel并管理起来（提供同步创建、异步创建、混合创建, 这里其实是按一定规则去调用 ConnectionFactory）
        ```
         */
        this.connectionManager = new DefaultClientConnectionManager(connectionSelectStrategy,
            new RpcConnectionFactory(userProcessors, this), connectionEventHandler,
            connectionEventListener, switches());
        this.connectionManager.setAddressParser(this.addressParser);
        this.connectionManager.startup();

        /**
         * 第二步
         * notes 创建真正的请求执行客户端（发起调用类）
         */
        this.rpcRemoting = new RpcClientRemoting(new RpcCommandFactory(), this.addressParser, this.connectionManager);


        this.taskScanner.add(this.connectionManager);
        this.taskScanner.startup();

        if (switches().isOn(GlobalSwitch.CONN_MONITOR_SWITCH)) {
            if (monitorStrategy == null) {
                connectionMonitor = new DefaultConnectionMonitor(new ScheduledDisconnectStrategy(),
                    this.connectionManager);
            } else {
                connectionMonitor = new DefaultConnectionMonitor(monitorStrategy,
                    this.connectionManager);
            }
            connectionMonitor.startup();
            logger.warn("Switch on connection monitor");
        }
        if (switches().isOn(GlobalSwitch.CONN_RECONNECT_SWITCH)) {
            reconnectManager = new ReconnectManager(connectionManager);
            reconnectManager.startup();

            connectionEventHandler.setReconnector(reconnectManager);
            logger.warn("Switch on reconnect manager");
        }
    }

    /**
     * ==================================================start RpcRemoting =============================================
     * 下面的一大块代码都是对 {@link #rpcRemoting} 的方法的调用
     *
     */


    @Override
    public void oneway(final String address, final Object request) throws RemotingException,
                                                                  InterruptedException {
        this.rpcRemoting.oneway(address, request, null);
    }

    @Override
    public void oneway(final String address, final Object request, final InvokeContext invokeContext)
                                                                                                     throws RemotingException,
                                                                                                     InterruptedException {
        this.rpcRemoting.oneway(address, request, invokeContext);
    }

    @Override
    public void oneway(final Url url, final Object request) throws RemotingException,
                                                           InterruptedException {
        this.rpcRemoting.oneway(url, request, null);
    }

    @Override
    public void oneway(final Url url, final Object request, final InvokeContext invokeContext)
                                                                                              throws RemotingException,
                                                                                              InterruptedException {
        this.rpcRemoting.oneway(url, request, invokeContext);
    }

    @Override
    public void oneway(final Connection conn, final Object request) throws RemotingException {
        this.rpcRemoting.oneway(conn, request, null);
    }

    @Override
    public void oneway(final Connection conn, final Object request,
                       final InvokeContext invokeContext) throws RemotingException {
        this.rpcRemoting.oneway(conn, request, invokeContext);
    }

    @Override
    public Object invokeSync(final String address, final Object request, final int timeoutMillis)
                                                                                                 throws RemotingException,
                                                                                                 InterruptedException {
        return this.rpcRemoting.invokeSync(address, request, null, timeoutMillis);
    }

    @Override
    public Object invokeSync(final String address, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
        return this.rpcRemoting.invokeSync(address, request, invokeContext, timeoutMillis);
    }

    @Override
    public Object invokeSync(final Url url, final Object request, final int timeoutMillis)
                                                                                          throws RemotingException,
                                                                                          InterruptedException {
        return this.invokeSync(url, request, null, timeoutMillis);
    }

    @Override
    public Object invokeSync(final Url url, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
        return this.rpcRemoting.invokeSync(url, request, invokeContext, timeoutMillis);
    }

    @Override
    public Object invokeSync(final Connection conn, final Object request, final int timeoutMillis)
                                                                                                  throws RemotingException,
                                                                                                  InterruptedException {
        return this.rpcRemoting.invokeSync(conn, request, null, timeoutMillis);
    }

    @Override
    public Object invokeSync(final Connection conn, final Object request,
                             final InvokeContext invokeContext, final int timeoutMillis)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
        return this.rpcRemoting.invokeSync(conn, request, invokeContext, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final String address, final Object request,
                                              final int timeoutMillis) throws RemotingException,
                                                                      InterruptedException {
        return this.rpcRemoting.invokeWithFuture(address, request, null, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final String address, final Object request,
                                              final InvokeContext invokeContext,
                                              final int timeoutMillis) throws RemotingException,
                                                                      InterruptedException {
        return this.rpcRemoting.invokeWithFuture(address, request, invokeContext, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final Url url, final Object request,
                                              final int timeoutMillis) throws RemotingException,
                                                                      InterruptedException {
        return this.rpcRemoting.invokeWithFuture(url, request, null, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final Url url, final Object request,
                                              final InvokeContext invokeContext,
                                              final int timeoutMillis) throws RemotingException,
                                                                      InterruptedException {
        return this.rpcRemoting.invokeWithFuture(url, request, invokeContext, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final Connection conn, final Object request,
                                              int timeoutMillis) throws RemotingException {
        return this.rpcRemoting.invokeWithFuture(conn, request, null, timeoutMillis);
    }

    @Override
    public RpcResponseFuture invokeWithFuture(final Connection conn, final Object request,
                                              final InvokeContext invokeContext, int timeoutMillis)
                                                                                                   throws RemotingException {
        return this.rpcRemoting.invokeWithFuture(conn, request, invokeContext, timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final String address, final Object request,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        this.rpcRemoting.invokeWithCallback(address, request, null, invokeCallback, timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final String address, final Object request,
                                   final InvokeContext invokeContext,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        this.rpcRemoting.invokeWithCallback(address, request, invokeContext, invokeCallback,
            timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final Url url, final Object request,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        this.rpcRemoting.invokeWithCallback(url, request, null, invokeCallback, timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final Url url, final Object request,
                                   final InvokeContext invokeContext,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        this.rpcRemoting.invokeWithCallback(url, request, invokeContext, invokeCallback,
            timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final Connection conn, final Object request,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException {
        this.rpcRemoting.invokeWithCallback(conn, request, null, invokeCallback, timeoutMillis);
    }

    @Override
    public void invokeWithCallback(final Connection conn, final Object request,
                                   final InvokeContext invokeContext,
                                   final InvokeCallback invokeCallback, final int timeoutMillis)
                                                                                                throws RemotingException {
        this.rpcRemoting.invokeWithCallback(conn, request, invokeContext, invokeCallback,
            timeoutMillis);
    }

    /*
     * ================================================== end RpcRemoting =============================================
     */

    @Override
    public void addConnectionEventProcessor(ConnectionEventType type,
                                            ConnectionEventProcessor processor) {
        this.connectionEventListener.addConnectionEventProcessor(type, processor);
    }

    @Override
    public void registerUserProcessor(UserProcessor<?> processor) {
        UserProcessorRegisterHelper.registerUserProcessor(processor, this.userProcessors);
    }

    @Override
    public Connection createStandaloneConnection(String ip, int port, int connectTimeout)
                                                                                         throws RemotingException {
        return this.connectionManager.create(ip, port, connectTimeout);
    }

    @Override
    public Connection createStandaloneConnection(String address, int connectTimeout)
                                                                                    throws RemotingException {
        return this.connectionManager.create(address, connectTimeout);
    }

    @Override
    public void closeStandaloneConnection(Connection conn) {
        if (null != conn) {
            conn.close();
        }
    }

    @Override
    public Connection getConnection(String address, int connectTimeout) throws RemotingException,
                                                                       InterruptedException {
        Url url = this.addressParser.parse(address);
        return this.getConnection(url, connectTimeout);
    }

    @Override
    public Connection getConnection(Url url, int connectTimeout) throws RemotingException,
                                                                InterruptedException {
        url.setConnectTimeout(connectTimeout);
        return this.connectionManager.getAndCreateIfAbsent(url);
    }

    @Override
    public Map<String, List<Connection>> getAllManagedConnections() {
        return this.connectionManager.getAll();
    }

    @Override
    public boolean checkConnection(String address) {
        Url url = this.addressParser.parse(address);
        Connection conn = this.connectionManager.get(url.getUniqueKey());
        try {
            this.connectionManager.check(conn);
        } catch (Exception e) {
            logger.warn("check failed. connection: {}", conn, e);
            return false;
        }
        return true;
    }

    /**
     * Close all connections of a address
     *
     * @param addr
     */
    public void closeConnection(String addr) {
        Url url = this.addressParser.parse(addr);
        if (switches().isOn(GlobalSwitch.CONN_RECONNECT_SWITCH) && reconnectManager != null) {
            reconnectManager.disableReconnect(url);
        }
        this.connectionManager.remove(url.getUniqueKey());
    }

    @Override
    public void closeConnection(Url url) {
        if (switches().isOn(GlobalSwitch.CONN_RECONNECT_SWITCH) && reconnectManager != null) {
            reconnectManager.disableReconnect(url);
        }
        this.connectionManager.remove(url.getUniqueKey());
    }

    @Override
    public void enableConnHeartbeat(String address) {
        Url url = this.addressParser.parse(address);
        this.enableConnHeartbeat(url);
    }

    @Override
    public void enableConnHeartbeat(Url url) {
        if (null != url) {
            this.connectionManager.enableHeartbeat(this.connectionManager.get(url.getUniqueKey()));
        }
    }

    @Override
    public void disableConnHeartbeat(String address) {
        Url url = this.addressParser.parse(address);
        this.disableConnHeartbeat(url);
    }

    @Override
    public void disableConnHeartbeat(Url url) {
        if (null != url) {
            this.connectionManager.disableHeartbeat(this.connectionManager.get(url.getUniqueKey()));
        }
    }

    @Override
    public void enableReconnectSwitch() {
        this.switches().turnOn(GlobalSwitch.CONN_RECONNECT_SWITCH);
    }

    @Override
    public void disableReconnectSwith() {
        this.switches().turnOff(GlobalSwitch.CONN_RECONNECT_SWITCH);
    }

    @Override
    public boolean isReconnectSwitchOn() {
        return this.switches().isOn(GlobalSwitch.CONN_RECONNECT_SWITCH);
    }

    @Override
    public void enableConnectionMonitorSwitch() {
        this.switches().turnOn(GlobalSwitch.CONN_MONITOR_SWITCH);
    }

    @Override
    public void disableConnectionMonitorSwitch() {
        this.switches().turnOff(GlobalSwitch.CONN_MONITOR_SWITCH);
    }

    @Override
    public boolean isConnectionMonitorSwitchOn() {
        return this.switches().isOn(GlobalSwitch.CONN_MONITOR_SWITCH);
    }

    @Override
    public DefaultConnectionManager getConnectionManager() {
        return this.connectionManager;
    }

    @Override
    public RemotingAddressParser getAddressParser() {
        return this.addressParser;
    }

    @Override
    public void setAddressParser(RemotingAddressParser addressParser) {
        this.addressParser = addressParser;
    }

    @Override
    public void setMonitorStrategy(ConnectionMonitorStrategy monitorStrategy) {
        this.monitorStrategy = monitorStrategy;
    }
}
