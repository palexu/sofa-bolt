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

import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.connection.ConnectionFactory;
import com.alipay.remoting.constant.Constants;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.FutureTaskUtil;
import com.alipay.remoting.util.RunStateRecordedFutureTask;
import com.alipay.remoting.util.StringUtils;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 * Abstract implementation of connection manager
 * notes channel管理器
 *  创建channel的过程是异步的， 主要通过这个{@link ConnectionPoolCall}来进行。
 *  # 建立多连接与连接预热
 *      通常来说，点对点的直连通信，客户端和服务端，一个 IP 一个连接对象就够用了。不管是吞吐能力还是并发度，都能满足一般业务的通信需求。
 *      而有一些场景，比如不是点对点直连通信，而是经过了 LVS VIP，或者 F5 设备的连接，此时，为了负载均衡和容错，会针对一个 URL 地址建立多个连接。
 *      我们提供如下方式来建立多连接，即发起调用时传入的 URL 增加如下参数 127.0.0.1:12200?_CONNECTIONNUM=30&_CONNECTIONWARMUP=true，
 *      表示针对这个 IP 地址，需要建立30个连接，同时需要预热连接。其中预热与不预热的区别是：
 *      - 预热：即第一次调用（比如 Sync 同步调用），就建立30个连接
 *      - 不预热：每一次调用，创建一个连接，直到创建满30个连接
 *
 * notes 管理器没有连接池么？难道每次都重新创建连接？
 *  > 有的 {@link ConnectionPool}
 *
 * todo 什么时候第一次创建channel？第一次要发消息的时候？
 *  > 是的，通过 {@link #create}
 *
 * @author xiaomin.cxm
 * @version $Id: DefaultConnectionManager.java, v 0.1 Mar 8, 2016 10:43:51 AM xiaomin.cxm Exp $
 */
public class DefaultConnectionManager extends AbstractLifeCycle implements ConnectionManager,
                                                               ConnectionHeartbeatManager,
                                                               Scannable, LifeCycle {

    private static final Logger                                                     logger = BoltLoggerFactory
                                                                                               .getLogger("CommonDefault");

    /**
     * executor to create connections in async way
     * notes 用于异步创建连接
     */
    private ThreadPoolExecutor                                                      asyncCreateConnectionExecutor;

    /**
     * switch status
     */
    private GlobalSwitch                                                            globalSwitch;

    /**
     *notes
     * connection pool initialize tasks
     * key： url+port+协议等等， 唯一标志一个资源
     * value：资源对应的连接池(因为可能是异步创建， 因此通过 Future来访问）
     * 唯一的一个put操作： {@link #getConnectionPoolAndCreateIfAbsent}
     * 其他方法里的调用， 均属于get或remove或isEmpty等
     */
    protected ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>> connTasks;

    /**
     *notes
     * heal connection tasks
     * {@link #createConnectionAndHealIfNeed} 在向{@link #connTasks}里put时，如果有需要， 可以指定进行heal操作
     * heal -> 把连接池补满
     *  // only when async creating connections done
     *  // and the actual size of connections less than expected, the healing task can be run.
     */
    protected ConcurrentHashMap<String, FutureTask<Integer>>                        healTasks;

    /**
     * connection pool select strategy
     */
    protected ConnectionSelectStrategy                                              connectionSelectStrategy;

    /**
     * address parser
     */
    protected RemotingAddressParser                                                 addressParser;

    /**
     * connection factory
     */
    protected ConnectionFactory                                                     connectionFactory;

    /**
     * connection event handler
     */
    protected ConnectionEventHandler                                                connectionEventHandler;

    /**
     * connection event listener
     */
    protected ConnectionEventListener                                               connectionEventListener;

    /**
     * Default constructor.
     */
    public DefaultConnectionManager() {
        this.connTasks = new ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>>();
        this.healTasks = new ConcurrentHashMap<String, FutureTask<Integer>>();
        this.connectionSelectStrategy = new RandomSelectStrategy(globalSwitch);
    }

    /**
     * Construct with parameters.
     *
     * @param connectionSelectStrategy connection selection strategy
     */
    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy) {
        this();
        this.connectionSelectStrategy = connectionSelectStrategy;
    }

    /**
     * Construct with parameters.
     *
     * @param connectionSelectStrategy connection selection strategy
     * @param connectionFactory connection factory
     */
    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,
                                    ConnectionFactory connectionFactory) {
        this(connectionSelectStrategy);
        this.connectionFactory = connectionFactory;
    }

    /**
     * Construct with parameters.
     * @param connectionFactory connection selection strategy
     * @param addressParser address parser
     * @param connectionEventHandler connection event handler
     */
    public DefaultConnectionManager(ConnectionFactory connectionFactory,
                                    RemotingAddressParser addressParser,
                                    ConnectionEventHandler connectionEventHandler) {
        this(new RandomSelectStrategy(null), connectionFactory);
        this.addressParser = addressParser;
        this.connectionEventHandler = connectionEventHandler;
    }

    /**
     * Construct with parameters.
     *
     * @param connectionSelectStrategy connection selection strategy
     * @param connectionFactory connection factory
     * @param connectionEventHandler connection event handler
     * @param connectionEventListener connection event listener
     */
    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,
                                    ConnectionFactory connectionFactory,
                                    ConnectionEventHandler connectionEventHandler,
                                    ConnectionEventListener connectionEventListener) {
        this(connectionSelectStrategy, connectionFactory);
        this.connectionEventHandler = connectionEventHandler;
        this.connectionEventListener = connectionEventListener;
    }

    /**
     * Construct with parameters.
     *
     * @param connectionSelectStrategy connection selection strategy.
     * @param connectionFactory connection factory
     * @param connectionEventHandler connection event handler
     * @param connectionEventListener connection event listener
     * @param globalSwitch global switch
     */
    public DefaultConnectionManager(ConnectionSelectStrategy connectionSelectStrategy,
                                    ConnectionFactory connectionFactory,
                                    ConnectionEventHandler connectionEventHandler,
                                    ConnectionEventListener connectionEventListener,
                                    GlobalSwitch globalSwitch) {
        this(connectionSelectStrategy, connectionFactory, connectionEventHandler,
            connectionEventListener);
        this.globalSwitch = globalSwitch;
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        long keepAliveTime = ConfigManager.conn_create_tp_keepalive();
        int queueSize = ConfigManager.conn_create_tp_queue_size();
        int minPoolSize = ConfigManager.conn_create_tp_min_size();
        int maxPoolSize = ConfigManager.conn_create_tp_max_size();
        this.asyncCreateConnectionExecutor = new ThreadPoolExecutor(minPoolSize, maxPoolSize,
            keepAliveTime, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(queueSize),
            new NamedThreadFactory("Bolt-conn-warmup-executor", true));
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();

        if (asyncCreateConnectionExecutor != null) {
            asyncCreateConnectionExecutor.shutdown();
        }

        if (null == this.connTasks || this.connTasks.isEmpty()) {
            return;
        }
        Iterator<String> iter = this.connTasks.keySet().iterator();
        while (iter.hasNext()) {
            String poolKey = iter.next();
            this.removeTask(poolKey);
            iter.remove();
        }
        logger.warn("All connection pool and connections have been removed!");
    }

    @Deprecated
    @Override
    public void init() {
        this.connectionEventHandler.setConnectionManager(this);
        this.connectionEventHandler.setConnectionEventListener(connectionEventListener);
        this.connectionFactory.init(connectionEventHandler);
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#add(com.alipay.remoting.Connection)
     */
    @Override
    public void add(Connection connection) {
        Set<String> poolKeys = connection.getPoolKeys();
        for (String poolKey : poolKeys) {
            this.add(connection, poolKey);
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#add(com.alipay.remoting.Connection, java.lang.String)
     */
    @Override
    public void add(Connection connection, String poolKey) {
        ConnectionPool pool = null;
        try {
            // get or create an empty connection pool
            pool = this.getConnectionPoolAndCreateIfAbsent(poolKey, new ConnectionPoolCall());
        } catch (Exception e) {
            // should not reach here.
            logger.error(
                "[NOTIFYME] Exception occurred when getOrCreateIfAbsent an empty ConnectionPool!",
                e);
        }
        if (pool != null) {
            pool.add(connection);
        } else {
            // should not reach here.
            logger.error("[NOTIFYME] Connection pool NULL!");
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#get(String)
     */
    @Override
    public Connection get(String poolKey) {
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        return null == pool ? null : pool.get();
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#getAll(java.lang.String)
     */
    @Override
    public List<Connection> getAll(String poolKey) {
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        return null == pool ? new ArrayList<Connection>() : pool.getAll();
    }

    /**
     * Get all connections of all poolKey.
     *
     * @return a map with poolKey as key and a list of connections in ConnectionPool as value
     */
    @Override
    public Map<String, List<Connection>> getAll() {
        Map<String, List<Connection>> allConnections = new HashMap<String, List<Connection>>();
        Iterator<Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>>> iterator = this
            .getConnPools().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, RunStateRecordedFutureTask<ConnectionPool>> entry = iterator.next();
            ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(entry.getValue(), logger);
            if (null != pool) {
                allConnections.put(entry.getKey(), pool.getAll());
            }
        }
        return allConnections;
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#remove(com.alipay.remoting.Connection)
     */
    @Override
    public void remove(Connection connection) {
        if (null == connection) {
            return;
        }
        Set<String> poolKeys = connection.getPoolKeys();
        if (null == poolKeys || poolKeys.isEmpty()) {
            connection.close();
            logger.warn("Remove and close a standalone connection.");
        } else {
            for (String poolKey : poolKeys) {
                this.remove(connection, poolKey);
            }
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#remove(com.alipay.remoting.Connection, java.lang.String)
     */
    @Override
    public void remove(Connection connection, String poolKey) {
        if (null == connection || StringUtils.isBlank(poolKey)) {
            return;
        }
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        if (null == pool) {
            connection.close();
            logger.warn("Remove and close a standalone connection.");
        } else {
            pool.removeAndTryClose(connection);
            if (pool.isEmpty()) {
                this.removeTask(poolKey);
                logger.warn(
                    "Remove and close the last connection in ConnectionPool with poolKey {}",
                    poolKey);
            } else {
                logger
                    .warn(
                        "Remove and close a connection in ConnectionPool with poolKey {}, {} connections left.",
                        poolKey, pool.size());
            }
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#remove(java.lang.String)
     */
    @Override
    public void remove(String poolKey) {
        if (StringUtils.isBlank(poolKey)) {
            return;
        }

        RunStateRecordedFutureTask<ConnectionPool> task = this.connTasks.remove(poolKey);
        if (null != task) {
            ConnectionPool pool = this.getConnectionPool(task);
            if (null != pool) {
                pool.removeAllAndTryClose();
                logger.warn("Remove and close all connections in ConnectionPool of poolKey={}",
                    poolKey);
            }
        }
    }

    /**
     * Warning! This is weakly consistent implementation, to prevent lock the whole {@link ConcurrentHashMap}.
     */
    @Deprecated
    @Override
    public void removeAll() {
        if (null == this.connTasks || this.connTasks.isEmpty()) {
            return;
        }
        Iterator<String> iter = this.connTasks.keySet().iterator();
        while (iter.hasNext()) {
            String poolKey = iter.next();
            this.removeTask(poolKey);
            iter.remove();
        }
        logger.warn("All connection pool and connections have been removed!");
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#check(com.alipay.remoting.Connection)
     */
    @Override
    public void check(Connection connection) throws RemotingException {
        if (connection == null) {
            throw new RemotingException("Connection is null when do check!");
        }
        if (connection.getChannel() == null || !connection.getChannel().isActive()) {
            this.remove(connection);
            throw new RemotingException("Check connection failed for address: "
                                        + connection.getUrl());
        }
        if (!connection.getChannel().isWritable()) {
            // No remove. Most of the time it is unwritable temporarily.
            throw new RemotingException("Check connection failed for address: "
                                        + connection.getUrl() + ", maybe write overflow!");
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#count(java.lang.String)
     */
    @Override
    public int count(String poolKey) {
        if (StringUtils.isBlank(poolKey)) {
            return 0;
        }
        ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
        if (null != pool) {
            return pool.size();
        } else {
            return 0;
        }
    }

    /**
     * in case of cache pollution and connection leak, to do schedule scan
     */
    @Override
    public void scan() {
        if (null != this.connTasks && !this.connTasks.isEmpty()) {
            Iterator<String> iter = this.connTasks.keySet().iterator();
            while (iter.hasNext()) {
                String poolKey = iter.next();
                ConnectionPool pool = this.getConnectionPool(this.connTasks.get(poolKey));
                if (null != pool) {
                    pool.scan();
                    if (pool.isEmpty()) {
                        if ((System.currentTimeMillis() - pool.getLastAccessTimestamp()) > Constants.DEFAULT_EXPIRE_TIME) {
                            iter.remove();
                            logger.warn("Remove expired pool task of poolKey {} which is empty.",
                                poolKey);
                        }
                    }
                }
            }
        }
    }

    /**
     * If no task cached, create one and initialize the connections.
     *
     * @see ConnectionManager#getAndCreateIfAbsent(Url)
     */
    @Override
    public Connection getAndCreateIfAbsent(Url url) throws InterruptedException, RemotingException {
        // get and create a connection pool with initialized connections.
        ConnectionPool pool = this.getConnectionPoolAndCreateIfAbsent(url.getUniqueKey(),
            new ConnectionPoolCall(url));
        if (null != pool) {
            return pool.get();
        } else {
            logger.error("[NOTIFYME] bug detected! pool here must not be null!");
            return null;
        }
    }

    /**
     * If no task cached, create one and initialize the connections.
     * If task cached, check whether the number of connections adequate, if not then heal it.
     *
     * @param url target url
     * @throws InterruptedException
     * @throws RemotingException
     */
    @Override
    public void createConnectionAndHealIfNeed(Url url) throws InterruptedException,
                                                      RemotingException {
        // get and create a connection pool with initialized connections.
        ConnectionPool pool = this.getConnectionPoolAndCreateIfAbsent(url.getUniqueKey(),
            new ConnectionPoolCall(url));
        if (null != pool) {
            healIfNeed(pool, url);
        } else {
            logger.error("[NOTIFYME] bug detected! pool here must not be null!");
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#create(com.alipay.remoting.Url)
     */
    @Override
    public Connection create(Url url) throws RemotingException {
        Connection conn;
        try {
            conn = this.connectionFactory.createConnection(url);
        } catch (Exception e) {
            throw new RemotingException("Create connection failed. The address is "
                                        + url.getOriginUrl(), e);
        }
        return conn;
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#create(java.lang.String, int, int)
     */
    @Override
    public Connection create(String ip, int port, int connectTimeout) throws RemotingException {
        try {
            return this.connectionFactory.createConnection(ip, port, connectTimeout);
        } catch (Exception e) {
            throw new RemotingException("Create connection failed. The address is " + ip + ":"
                                        + port, e);
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionManager#create(java.lang.String, int)
     */
    @Override
    public Connection create(String address, int connectTimeout) throws RemotingException {
        Url url = this.addressParser.parse(address);
        url.setConnectTimeout(connectTimeout);
        return create(url);
    }

    /**
     * @see com.alipay.remoting.ConnectionHeartbeatManager#disableHeartbeat(com.alipay.remoting.Connection)
     */
    @Override
    public void disableHeartbeat(Connection connection) {
        if (null != connection) {
            connection.getChannel().attr(Connection.HEARTBEAT_SWITCH).set(false);
        }
    }

    /**
     * @see com.alipay.remoting.ConnectionHeartbeatManager#enableHeartbeat(com.alipay.remoting.Connection)
     */
    @Override
    public void enableHeartbeat(Connection connection) {
        if (null != connection) {
            connection.getChannel().attr(Connection.HEARTBEAT_SWITCH).set(true);
        }
    }

    /**
     * get connection pool from future task
     *
     * @param task future task
     * @return connection pool
     */
    private ConnectionPool getConnectionPool(RunStateRecordedFutureTask<ConnectionPool> task) {
        return FutureTaskUtil.getFutureTaskResult(task, logger);

    }

    /**
     * Get the mapping instance of {@link ConnectionPool} with the specified poolKey,
     * or create one if there is none mapping in connTasks.
     *
     * @param poolKey  mapping key of {@link ConnectionPool}
     * @param callable the callable task
     * @return a non-nullable instance of {@link ConnectionPool}
     * @throws RemotingException if there is no way to get an available {@link ConnectionPool}
     * @throws InterruptedException
     */
    private ConnectionPool getConnectionPoolAndCreateIfAbsent(String poolKey,
                                                              Callable<ConnectionPool> callable)
                                                                                                throws RemotingException,
                                                                                                InterruptedException {
        RunStateRecordedFutureTask<ConnectionPool> initialTask;
        ConnectionPool pool = null;

        int retry = Constants.DEFAULT_RETRY_TIMES;

        int timesOfResultNull = 0;
        int timesOfInterrupt = 0;

        for (int i = 0; (i < retry) && (pool == null); ++i) {

            initialTask = this.connTasks.get(poolKey);

            //todo 这里是进行了双重检查？  为了线程安全？

            //说明之前没有调用过这个地址
            if (null == initialTask) {
                //那么尝试执行新建连接的任务
                RunStateRecordedFutureTask<ConnectionPool> newTask = new RunStateRecordedFutureTask<ConnectionPool>(callable);
                initialTask = this.connTasks.putIfAbsent(poolKey, newTask);

                //说明之前没有提交过任务， 那么现在执行任务
                if (null == initialTask) {
                    initialTask = newTask;
                    /**
                    notes 为什么这里没有提交到线程池， 而是直接 .run() 执行了？
                     而且内部调用的 ConnectionFactory， 也是同步阻塞地执行连接操作。
                     也就说说整个新建连接的过程全程都是同步阻塞的， 这是为啥呢？
                     > emm，还是有异步创建连接的需求的，see {@link #doCreate}
                       所以是为了在"同一个地方"进行"同步/异步"的控制，而这个地方， bolt把它定在 {@link #doCreate}
                     */
                    initialTask.run();
                }
            }

            try {
                pool = initialTask.get();
                if (null == pool) {
                    if (i + 1 < retry) {
                        timesOfResultNull++;
                        //还有重试机会时，继续重试
                        continue;
                    }
                    this.connTasks.remove(poolKey);
                    String errMsg = "Get future task result null for poolKey [" + poolKey
                                    + "] after [" + (timesOfResultNull + 1) + "] times try.";
                    throw new RemotingException(errMsg);
                }
            } catch (InterruptedException e) {
                if (i + 1 < retry) {
                    timesOfInterrupt++;
                    continue;// retry if interrupted
                }
                this.connTasks.remove(poolKey);
                logger.warn("Future task of poolKey {} interrupted {} times. InterruptedException thrown and stop retry.", poolKey, (timesOfInterrupt + 1), e);
                throw e;
            } catch (ExecutionException e) {
                // DO NOT retry if ExecutionException occurred
                this.connTasks.remove(poolKey);

                Throwable cause = e.getCause();
                if (cause instanceof RemotingException) {
                    throw (RemotingException) cause;
                } else {
                    FutureTaskUtil.launderThrowable(cause);
                }
            }
        }
        return pool;
    }

    /**
     * remove task and remove all connections
     *
     * @param poolKey target pool key
     */
    protected void removeTask(String poolKey) {
        RunStateRecordedFutureTask<ConnectionPool> task = this.connTasks.remove(poolKey);
        if (null != task) {
            ConnectionPool pool = FutureTaskUtil.getFutureTaskResult(task, logger);
            if (null != pool) {
                pool.removeAllAndTryClose();
            }
        }
    }

    /**
     * execute heal connection tasks if the actual number of connections in pool is less than expected
     *
     * @param pool connection pool
     * @param url target url
     */
    private void healIfNeed(ConnectionPool pool, Url url) throws RemotingException,
                                                         InterruptedException {
        String poolKey = url.getUniqueKey();
        // only when async creating connections done
        // and the actual size of connections less than expected, the healing task can be run.
        if (pool.isAsyncCreationDone() && pool.size() < url.getConnNum()) {
            FutureTask<Integer> task = this.healTasks.get(poolKey);
            if (null == task) {
                FutureTask<Integer> newTask = new FutureTask<Integer>(new HealConnectionCall(url,
                    pool));
                task = this.healTasks.putIfAbsent(poolKey, newTask);
                if (null == task) {
                    task = newTask;
                    task.run();
                }
            }
            try {
                int numAfterHeal = task.get();
                if (logger.isDebugEnabled()) {
                    logger.debug("[NOTIFYME] - conn num after heal {}, expected {}, warmup {}",
                        numAfterHeal, url.getConnNum(), url.isConnWarmup());
                }
            } catch (InterruptedException e) {
                this.healTasks.remove(poolKey);
                throw e;
            } catch (ExecutionException e) {
                this.healTasks.remove(poolKey);
                Throwable cause = e.getCause();
                if (cause instanceof RemotingException) {
                    throw (RemotingException) cause;
                } else {
                    FutureTaskUtil.launderThrowable(cause);
                }
            }
            // heal task is one-off, remove from cache directly after run
            this.healTasks.remove(poolKey);
        }
    }

    /**
     * a callable definition for initialize {@link ConnectionPool}
     *
     * @author tsui
     * @version $Id: ConnectionPoolCall.java, v 0.1 Mar 8, 2016 10:43:51 AM xiaomin.cxm Exp $
     */
    private class ConnectionPoolCall implements Callable<ConnectionPool> {
        private boolean whetherInitConnection;
        private Url     url;

        /**
         * create a {@link ConnectionPool} but not init connections
         */
        public ConnectionPoolCall() {
            this.whetherInitConnection = false;
        }

        /**
         * create a {@link ConnectionPool} and init connections with the specified {@link Url}
         *
         * @param url target url
         */
        public ConnectionPoolCall(Url url) {
            this.whetherInitConnection = true;
            this.url = url;
        }

        @Override
        public ConnectionPool call() throws Exception {
            final ConnectionPool pool = new ConnectionPool(connectionSelectStrategy);
            if (whetherInitConnection) {
                try {
                    doCreate(this.url, pool, this.getClass().getSimpleName(), 1);
                } catch (Exception e) {
                    pool.removeAllAndTryClose();
                    throw e;
                }
            }
            return pool;
        }

    }

    /**
     * a callable definition for healing connections in {@link ConnectionPool}
     *
     * @author tsui
     * @version $Id: HealConnectionCall.java, v 0.1 Jul 20, 2017 10:23:23 AM xiaomin.cxm Exp $
     */
    private class HealConnectionCall implements Callable<Integer> {
        private Url            url;
        private ConnectionPool pool;

        /**
         * create a {@link ConnectionPool} and init connections with the specified {@link Url}
         *
         * @param url target url
         */
        public HealConnectionCall(Url url, ConnectionPool pool) {
            this.url = url;
            this.pool = pool;
        }

        @Override
        public Integer call() throws Exception {
            doCreate(this.url, this.pool, this.getClass().getSimpleName(), 0);
            return this.pool.size();
        }
    }

    /**
     * do create connections
     *
     * @param url target url
     * @param pool connection pool
     * @param taskName task name
     * notes @param syncCreateNumWhenNotWarmup you can specify this param to ensure at least desired number of connections available in sync way
     * @throws RemotingException
     */
    private void doCreate(final Url url, final ConnectionPool pool, final String taskName,
                          final int syncCreateNumWhenNotWarmup) throws RemotingException {
        final int actualNum = pool.size();
        final int expectNum = url.getConnNum();
        if (actualNum >= expectNum) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("actual num {}, expect num {}, task name {}", actualNum, expectNum,
                taskName);
        }

        /**
         * notes 如果是预热，那么一次创建所有连接
         *  see {@link DefaultConnectionManager}
         */
        if (url.isConnWarmup()) {
            for (int i = actualNum; i < expectNum; ++i) {
                Connection connection = create(url);
                pool.add(connection);
            }
        }

        /**
         * notes 否则一次创建一条， 直到达到上限
         */
        else {
            if (syncCreateNumWhenNotWarmup < 0 || syncCreateNumWhenNotWarmup > url.getConnNum()) {
                throw new IllegalArgumentException(
                    "sync create number when not warmup should be [0," + url.getConnNum() + "]");
            }

            // create connection in sync way
            // notes 第一步：同步地创建指定数量的线程. 目的是为了保证当调用完这个方法后，已经存在指定数量的可用连接。防止外部开始调用了， 结果无连接可用的尴尬局面😅。
            if (syncCreateNumWhenNotWarmup > 0) {
                for (int i = 0; i < syncCreateNumWhenNotWarmup; ++i) {
                    Connection connection = create(url);
                    pool.add(connection);
                }
                if (syncCreateNumWhenNotWarmup >= url.getConnNum()) {
                    return;
                }
            }

            // notes 第二步：剩余的连接线程异步创建
            pool.markAsyncCreationStart();// mark the start of async
            try {
                this.asyncCreateConnectionExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (int i = pool.size(); i < url.getConnNum(); ++i) {
                                Connection conn = null;
                                try {
                                    conn = create(url);
                                } catch (RemotingException e) {
                                    logger
                                        .error(
                                            "Exception occurred in async create connection thread for {}, taskName {}",
                                            url.getUniqueKey(), taskName, e);
                                }
                                pool.add(conn);
                            }
                        } finally {
                            pool.markAsyncCreationDone();// mark the end of async
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                pool.markAsyncCreationDone();// mark the end of async when reject
                throw e;
            }
        } // end of NOT warm up
    }

    /**
     * Getter method for property <tt>connectionSelectStrategy</tt>.
     *
     * @return property value of connectionSelectStrategy
     */
    public ConnectionSelectStrategy getConnectionSelectStrategy() {
        return connectionSelectStrategy;
    }

    /**
     * Setter method for property <tt>connectionSelectStrategy</tt>.
     *
     * @param connectionSelectStrategy value to be assigned to property connectionSelectStrategy
     */
    public void setConnectionSelectStrategy(ConnectionSelectStrategy connectionSelectStrategy) {
        this.connectionSelectStrategy = connectionSelectStrategy;
    }

    /**
     * Getter method for property <tt>addressParser</tt>.
     *
     * @return property value of addressParser
     */
    public RemotingAddressParser getAddressParser() {
        return addressParser;
    }

    /**
     * Setter method for property <tt>addressParser</tt>.
     *
     * @param addressParser value to be assigned to property addressParser
     */
    public void setAddressParser(RemotingAddressParser addressParser) {
        this.addressParser = addressParser;
    }

    /**
     * Getter method for property <tt>connctionFactory</tt>.
     *
     * @return property value of connctionFactory
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Setter method for property <tt>connctionFactory</tt>.
     *
     * @param connectionFactory value to be assigned to property connctionFactory
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Getter method for property <tt>connectionEventHandler</tt>.
     *
     * @return property value of connectionEventHandler
     */
    public ConnectionEventHandler getConnectionEventHandler() {
        return connectionEventHandler;
    }

    /**
     * Setter method for property <tt>connectionEventHandler</tt>.
     *
     * @param connectionEventHandler value to be assigned to property connectionEventHandler
     */
    public void setConnectionEventHandler(ConnectionEventHandler connectionEventHandler) {
        this.connectionEventHandler = connectionEventHandler;
    }

    /**
     * Getter method for property <tt>connectionEventListener</tt>.
     *
     * @return property value of connectionEventListener
     */
    public ConnectionEventListener getConnectionEventListener() {
        return connectionEventListener;
    }

    /**
     * Setter method for property <tt>connectionEventListener</tt>.
     *
     * @param connectionEventListener value to be assigned to property connectionEventListener
     */
    public void setConnectionEventListener(ConnectionEventListener connectionEventListener) {
        this.connectionEventListener = connectionEventListener;
    }

    /**
     * Getter method for property <tt>connPools</tt>.
     *
     * @return property value of connPools
     */
    public ConcurrentHashMap<String, RunStateRecordedFutureTask<ConnectionPool>> getConnPools() {
        return this.connTasks;
    }
}
