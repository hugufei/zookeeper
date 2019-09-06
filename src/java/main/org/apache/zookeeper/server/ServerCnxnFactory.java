/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry;

import javax.management.JMException;

import org.apache.zookeeper.Login;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.auth.SaslServerCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ServerCnxnFactory {

    public static final String ZOOKEEPER_SERVER_CNXN_FACTORY = "zookeeper.serverCnxnFactory";

    public interface PacketProcessor {
        public void processPacket(ByteBuffer packet, ServerCnxn src);
    }
    
    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxnFactory.class);

    // sessionMap is used to speed up closeSession()
    // 每个session对应的ServerCnxn对象
    protected final ConcurrentMap<Long, ServerCnxn> sessionMap = new ConcurrentHashMap<Long, ServerCnxn>();

    /**
     * The buffer will cause the connection to be close when we do a send.
     */
    // 代表关闭的请求
    static final ByteBuffer closeConn = ByteBuffer.allocate(0);

    // 本地端口
    public abstract int getLocalPort();

    //所有ServerCnxn的迭代器
    public abstract Iterable<ServerCnxn> getConnections();

    // 获取活动的连接数
    public int getNumAliveConnections() {
        synchronized(cnxns) {
            return cnxns.size();
        }
    }

    ZooKeeperServer getZooKeeperServer() {
        return zkServer;
    }

    // 关闭某个session
    // 从NIOServerCnxnFactory找到该会话对应的NIOServerCnxn，将其关闭。
    public abstract void closeSession(long sessionId);

    // 配置地址，以及最大client连接数等
    // 核心流程：初始化主线程，打开selector,并bind端口，打开NIO的Accept通知
    public abstract void configure(InetSocketAddress addr, int maxClientCnxns) throws IOException;

    protected SaslServerCallbackHandler saslServerCallbackHandler;

    public Login login;

    /** Maximum number of connections allowed from particular host (ip) */
    // 获取每个host的最大连接数,默认60
    public abstract int getMaxClientCnxnsPerHost();

    /** Maximum number of connections allowed from particular host (ip) */
    // 设置每个host的最大连接数
    public abstract void setMaxClientCnxnsPerHost(int max);

    // 单机版启动
    public abstract void startup(ZooKeeperServer zkServer) throws IOException, InterruptedException;

    // 等待线程结束
    public abstract void join() throws InterruptedException;

    // 关闭socket，线程
    public abstract void shutdown();

    // 集群版启动
    public abstract void start();

    //zk服务器
    protected ZooKeeperServer zkServer;


    final public void setZooKeeperServer(ZooKeeperServer zk) {
        this.zkServer = zk;
        if (zk != null) {
            zk.setServerCnxnFactory(this);
        }
    }

    // 关闭所有ServerCnxn
    public abstract void closeAll();

    // 创建工厂类
    static public ServerCnxnFactory createFactory() throws IOException {
        // 默认是NIOServerCnxnFactory
        String serverCnxnFactoryName = System.getProperty(ZOOKEEPER_SERVER_CNXN_FACTORY);
        if (serverCnxnFactoryName == null) {
            serverCnxnFactoryName = NIOServerCnxnFactory.class.getName();
        }
        //反射调用构造函数
        try {
            ServerCnxnFactory serverCnxnFactory = (ServerCnxnFactory) Class.forName(serverCnxnFactoryName)
                    .getDeclaredConstructor().newInstance();
            LOG.info("Using {} as server connection factory", serverCnxnFactoryName);
            return serverCnxnFactory;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate "
                    + serverCnxnFactoryName);
            ioe.initCause(e);
            throw ioe;
        }
    }

    //创建工厂，并配置地址和最大线程数
    static public ServerCnxnFactory createFactory(int clientPort,int maxClientCnxns) throws IOException {
        return createFactory(new InetSocketAddress(clientPort), maxClientCnxns);
    }

    //创建工厂，并配置地址和最大线程数
    static public ServerCnxnFactory createFactory(InetSocketAddress addr,int maxClientCnxns) throws IOException{
        ServerCnxnFactory factory = createFactory();
        factory.configure(addr, maxClientCnxns);
        return factory;
    }

    // 获取本地地址
    public abstract InetSocketAddress getLocalAddress();

    //每个ServerCnxn对应的jmx数据
    private final Map<ServerCnxn, ConnectionBean> connectionBeans = new ConcurrentHashMap<ServerCnxn, ConnectionBean>();

    // 所有ServerCnxn对象的集合
    protected final HashSet<ServerCnxn> cnxns = new HashSet<ServerCnxn>();

    //连接解绑（JMX?）
    public void unregisterConnection(ServerCnxn serverCnxn) {
        ConnectionBean jmxConnectionBean = connectionBeans.remove(serverCnxn);
        if (jmxConnectionBean != null){
            MBeanRegistry.getInstance().unregister(jmxConnectionBean);
        }
    }

    //连接绑定（JMX?）
    public void registerConnection(ServerCnxn serverCnxn) {
        if (zkServer != null) {
            ConnectionBean jmxConnectionBean = new ConnectionBean(serverCnxn, zkServer);
            try {
                MBeanRegistry.getInstance().register(jmxConnectionBean, zkServer.jmxServerBean);
                connectionBeans.put(serverCnxn, jmxConnectionBean);
            } catch (JMException e) {
                LOG.warn("Could not register connection", e);
            }
        }
    }

    // 添加会话信息
    public void addSession(long sessionId, ServerCnxn cnxn) {
        sessionMap.put(sessionId, cnxn);
    }

    /**
     * Initialize the server SASL if specified.
     *
     * If the user has specified a "ZooKeeperServer.LOGIN_CONTEXT_NAME_KEY"
     * or a jaas.conf using "java.security.auth.login.config"
     * the authentication is required and an exception is raised.
     * Otherwise no authentication is configured and no exception is raised.
     *
     * @throws IOException if jaas.conf is missing or there's an error in it.
     */
    protected void configureSaslLogin() throws IOException {
        String serverSection = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
                                                  ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);

        // Note that 'Configuration' here refers to javax.security.auth.login.Configuration.
        AppConfigurationEntry entries[] = null;
        SecurityException securityException = null;
        try {
            entries = Configuration.getConfiguration().getAppConfigurationEntry(serverSection);
        } catch (SecurityException e) {
            // handle below: might be harmless if the user doesn't intend to use JAAS authentication.
            securityException = e;
        }

        // No entries in jaas.conf
        // If there's a configuration exception fetching the jaas section and
        // the user has required sasl by specifying a LOGIN_CONTEXT_NAME_KEY or a jaas file
        // we throw an exception otherwise we continue without authentication.
        if (entries == null) {
            String jaasFile = System.getProperty(Environment.JAAS_CONF_KEY);
            String loginContextName = System.getProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY);
            if (securityException != null && (loginContextName != null || jaasFile != null)) {
                String errorMessage = "No JAAS configuration section named '" + serverSection +  "' was found";
                if (jaasFile != null) {
                    errorMessage += "in '" + jaasFile + "'.";
                }
                if (loginContextName != null) {
                    errorMessage += " But " + ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY + " was set.";
                }
                LOG.error(errorMessage);
                throw new IOException(errorMessage);
            }
            return;
        }

        // jaas.conf entry available
        try {
            saslServerCallbackHandler = new SaslServerCallbackHandler(Configuration.getConfiguration());
            login = new Login(serverSection, saslServerCallbackHandler);
            login.startThreadIfNeeded();
        } catch (LoginException e) {
            throw new IOException("Could not configure server because SASL configuration did not allow the "
              + " ZooKeeper server to authenticate itself properly: " + e);
        }
    }
}
