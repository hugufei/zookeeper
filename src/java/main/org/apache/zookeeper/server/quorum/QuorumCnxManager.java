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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 * 
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties. 
 * 
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 * 
 */

// Server选举Leader时的网络IO:QuorumCnxManager
// 每台服务器在启动的过程中，会启动一个QuorumPeerManager，负责各台服务器之间的底层Leader选举过程中的网络通信。
public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    // 接收队列的长度
    static final int RECV_CAPACITY = 100;
    // 发送队列的长度
    // 因为是选举leader投票，有特殊的要求:如果之前的票还没有投出去又产生了新的票，那么旧的票就可以直接作废了,不用真正的投出去
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;

    /*
     * Max buffer size to be read from the network.
     */
    static public final int maxBuffer = 2048;
    
    /*
     * Negative counter for observer server ids.
     */
    
    private AtomicLong observerCounter = new AtomicLong(-1);
    
    /*
     * Connection time out value in milliseconds 
     */
    
    private int cnxTO = 5000;
    
    /*
     * Local IP address
     */
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;
    final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections.synchronizedSet(new HashSet<Long>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    /*
     * Counter to count connection processing threads.
     */
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    // 每台目标服务器sid  对应的SendWorker
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    // 每台目标服务器sid  对应的需要发送的消息队列
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    // 每台目标服务器sid  上一次发送的内容
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    // 接收队列， 本台服务器接收到的消息
    public final ArrayBlockingQueue<Message> recvQueue;

    /*
     * Object to synchronize access to recvQueue
     */
    private final Object recvQLock = new Object();

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    // SendWorker 和 RecvWorker 工作线程的总数量
    private AtomicInteger threadCnt = new AtomicInteger(0);

    //Message类定义了消息结构，包含sid以及消息体ByteBuffer
    static public class Message {
        
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    public QuorumCnxManager(final long mySid,
                            Map<Long,QuorumPeer.QuorumServer> view,
                            QuorumAuthServer authServer,
                            QuorumAuthLearner authLearner,
                            int socketTimeout,
                            boolean listenOnAllIPs,
                            int quorumCnxnThreadsSize,
                            boolean quorumSaslAuthEnabled) {
        this(mySid, view, authServer, authLearner, socketTimeout, listenOnAllIPs,
                quorumCnxnThreadsSize, quorumSaslAuthEnabled, new ConcurrentHashMap<Long, SendWorker>());
    }

    // visible for testing
    public QuorumCnxManager(final long mySid,
                            Map<Long,QuorumPeer.QuorumServer> view,
                            QuorumAuthServer authServer,
                            QuorumAuthLearner authLearner,
                            int socketTimeout,
                            boolean listenOnAllIPs,
                            int quorumCnxnThreadsSize,
                            boolean quorumSaslAuthEnabled,
                            ConcurrentHashMap<Long, SendWorker> senderWorkerMap) {
        this.senderWorkerMap = senderWorkerMap;

        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();
        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if(cnxToValue != null){
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;

        initializeAuth(mySid, authServer, authLearner, quorumCnxnThreadsSize,
                quorumSaslAuthEnabled);

        // Starts listener thread that waits for connection requests 
        listener = new Listener();
    }

    private void initializeAuth(final long mySid,
            final QuorumAuthServer authServer,
            final QuorumAuthLearner authLearner,
            final int quorumCnxnThreadsSize,
            final boolean quorumSaslAuthEnabled) {
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;
        if (!this.quorumSaslAuthEnabled) {
            LOG.debug("Not initializing connection executor as quorum sasl auth is disabled");
            return;
        }

        // init connection executors
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup()
                : Thread.currentThread().getThreadGroup();
        ThreadFactory daemonThFactory = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r, "QuorumConnectionThread-"
                        + "[myid=" + mySid + "]-"
                        + threadIndex.getAndIncrement());
                return t;
            }
        };
        this.connectionExecutor = new ThreadPoolExecutor(3,
                quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }

    /**
     * Invokes initiateConnection for testing purposes
     * 
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening channel to server " + sid);
        }
        Socket sock = new Socket();
        setSockOpts(sock);
        sock.connect(QuorumPeer.viewToVotingView(view).get(sid).electionAddr,
                     cnxTO);
        initiateConnection(sock, sid);
    }
    
    /**
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    // 发送链接请求，连接请求发出时，如果对方sid比自己大，仅仅发送自己sid也就是一个long过去，然后close掉
    public void initiateConnection(final Socket sock, final Long sid) {
        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error("Exception while connecting, id: {}, addr: {}, closing learner connection",
                     new Object[] { sid, sock.getRemoteSocketAddress() }, e);
            closeSocket(sock);
            return;
        }
    }

    /**
     * Server will initiate the connection request to its peer server
     * asynchronously via separate connection thread.
     */
    //异步初始化链接
    public void initiateConnectionAsync(final Socket sock, final Long sid) {
        if(!inprogressConnections.add(sid)){
            // simply return as there is a connection request to
            // server 'sid' already in progress.
            LOG.debug("Connection request to server id: {} is already in progress, so skipping this request",
                    sid);
            closeSocket(sock);
            return;
        }
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReqThread(sock, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            // Imp: Safer side catching all type of exceptions and remove 'sid'
            // from inprogress connections. This is to avoid blocking further
            // connection requests from this 'sid' in case of errors.
            inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            closeSocket(sock);
        }
    }

    /**
     * Thread to send connection request to peer server.
     *
     * 发出链接线程
     */
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final Socket sock;
        final Long sid;
        QuorumConnectionReqThread(final Socket sock, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.sock = sock;
            this.sid = sid;
        }

        @Override
        public void run() {
            try{
                initiateConnection(sock, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }
    }
    /**
     *
     * 发出连接时，要求自己sid大，完成SendWorker和ReceiveWorker的构造以及线程启动，否则close
     * 接收连接时，要求自己sid小，完成SendWorker和ReceiveWorker的构造以及线程启动，否则close
     * 这样就保证了[sidn,sid1]这样的连接中，双方都有初始化两个worker
     * PS:
     * 每台服务器都有会去连其他服务器，但是两台服务器之间没必要去建立两条socket,所以规定
     * 如果我要去连的服务器的sid大于我自己的sid，则不进行连接，并且关闭刚刚创建的socket
     *
     */
    private boolean startConnection(Socket sock, Long sid)
            throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        //先发送本机sid
        try {
            // Sending id and challenge
            dout = new DataOutputStream(sock.getOutputStream());
            // 发送本机sid
            dout.writeLong(this.mySid);
            dout.flush();

            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // authenticate learner
        authLearner.authenticate(sock, view.get(sid).hostname);

        // If lost the challenge, then drop the new connection
        // 发送连接的时候，只让大sid给小sid发送，如果当前sid小，那就close掉
        if (sid > this.mySid) {
            LOG.info("Have smaller server identifier, so dropping the " +
                     "connection: (" + sid + ", " + this.mySid + ")");
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            // 能够建立连接了，就初始化SendWorker和RecvWorker并且启动
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            //SendWorker记录RecvWorker
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            //finish掉sid对应的SendWorker
            if(vsw != null)
                vsw.finish();

            //存储sid对应的SendWorker
            senderWorkerMap.put(sid, sw);
            //存储sid对应的消息队列
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));
            
            sw.start();
            rw.start();
            
            return true;    
            
        }
        return false;
    }

    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     * 
     */
    // 接收连接请求时，如果对方sid比自己小，那么close掉socket然后自己去连接对方sid
    // PS::::在接收连接请求时，sid小的一方初始化两个worker
    public void receiveConnection(final Socket sock) {
        DataInputStream din = null;
        try {
            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                     sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * Server receives a connection request and handles it asynchronously via
     * separate thread.
     */
    public void receiveConnectionAsync(final Socket sock) {
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                     sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * Thread to receive connection request from peer server.
     * 接受连接线程
     * --接收连接时，要求自己sid小，完成SendWorker和ReceiveWorker的构造以及线程启动，否则close
     */
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {
        private final Socket sock;
        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }
    }

    // 发出连接时，要求自己sid大，完成SendWorker和ReceiveWorker的构造以及线程启动，否则close
    // 接收连接时，要求自己sid小，完成SendWorker和ReceiveWorker的构造以及线程启动，否则close
    private void handleConnection(Socket sock, DataInputStream din)
            throws IOException {
        Long sid = null;
        try {
            // Read server id
            sid = din.readLong();
            if (sid < 0) { // this is not a server id but a protocol version (see ZOOKEEPER-1633)
                sid = din.readLong();

                // next comes the #bytes in the remainder of the message
                // note that 0 bytes is fine (old servers)
                int num_remaining_bytes = din.readInt();
                if (num_remaining_bytes < 0 || num_remaining_bytes > maxBuffer) {
                    LOG.error("Unreasonable buffer length: {}", num_remaining_bytes);
                    closeSocket(sock);
                    return;
                }
                byte[] b = new byte[num_remaining_bytes];

                // remove the remainder of the message from din
                int num_read = din.read(b);
                if (num_read != num_remaining_bytes) {
                    LOG.error("Read only " + num_read + " bytes out of " + num_remaining_bytes + " sent by server " + sid);
                }
            }
            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * Choose identifier at random. We need a value to identify
                 * the connection.
                 */
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer: " + sid);
            }
        } catch (IOException e) {
            closeSocket(sock);
            LOG.warn("Exception reading or writing challenge: " + e.toString());
            return;
        }

        // do authenticating learner
        LOG.debug("Authenticating learner server.id: {}", sid);
        authServer.authenticate(sock, din);

        //If wins the challenge, then close the new connection.
        // 如果自己id大，就close掉当前连接(当前是小sid发给大sid的连接)，自己再去连对方sid
        if (sid < this.mySid) { // sid是对方的sid
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            //关闭老的SendWorker
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server: " + sid);
            closeSocket(sock);
            connectOne(sid);

            // Otherwise start worker threads to receive data.
        } else {
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            //关闭老的SendWorker
            if(vsw != null)
                vsw.finish();
            
            senderWorkerMap.put(sid, sw);
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));
            
            sw.start();
            rw.start();
            
            return;
        }
    }

    /**
     * Processes invoke this message to queue a message to send. Currently, 
     * only leader election uses it.
     */
    // 生产消息入口
    // 将消息根据sid添加进recv队列或者send队列,间接调用send，recv的生产
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        if (this.mySid == sid) {
             b.position(0);
            // 如果是给自己发送的，则不需要经过网络，自己转成Message并加入到recvQueue队列中即可
            // 投票的时候会先给自己投一票
            addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else {
             /*
              * Start a new connection if doesn't have one already.
              *
              * 发送给其他服务器：加入到服务器所对应的发送队列中
              */
             ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY);
             ArrayBlockingQueue<ByteBuffer> bqExisting = queueSendMap.putIfAbsent(sid, bq);
             if (bqExisting != null) {
                 addToSendQueue(bqExisting, b);
             } else {
                 addToSendQueue(bq, b);
             }
            // 如果还没有连接到这个服务器就连一下
             connectOne(sid);
        }
    }
    
    /**
     * Try to establish a connection to server with id sid.
     * 
     *  @param sid  server id
     */
    // 连接一个sid的服务器
    synchronized public void connectOne(long sid){
        // 如果没有记录在sender的map里面,表示还没有连上
        if (!connectedToPeer(sid)){
            InetSocketAddress electionAddr;
            if (view.containsKey(sid)) {
                // 从配置文件获取对应sid机器的选举端口
                electionAddr = view.get(sid).electionAddr;
            } else {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
            try {

                LOG.debug("Opening channel to server " + sid);
                //新建一个Socket
                Socket sock = new Socket();
                setSockOpts(sock);
                //链接目标服务器的选举端口
                sock.connect(view.get(sid).electionAddr, cnxTO);
                LOG.debug("Connected to server " + sid);

                // Sends connection request asynchronously if the quorum
                // sasl authentication is enabled. This is required because
                // sasl server authentication process may take few seconds to
                // finish, this may delay next peer connection requests.
                if (quorumSaslAuthEnabled) {
                    //异步启动一个线程去链接
                    initiateConnectionAsync(sock, sid);
                } else {
                    // 同步链接
                    initiateConnection(sock, sid);
                }
            } catch (UnresolvedAddressException e) {
                // Sun doesn't include the address that causes this
                // exception to be thrown, also UAE cannot be wrapped cleanly
                // so we log the exception in order to capture this critical
                // detail.
                LOG.warn("Cannot open channel to " + sid
                        + " at election address " + electionAddr, e);
                // Resolve hostname for this server in case the
                // underlying ip address has changed.
                if (view.containsKey(sid)) {
                    view.get(sid).recreateSocketAddresses();
                }
                throw e;
            } catch (IOException e) {
                LOG.warn("Cannot open channel to " + sid
                        + " at election address " + electionAddr,
                        e);
                // We can't really tell if the server is actually down or it failed
                // to connect to the server because the underlying IP address
                // changed. Resolve the hostname again just in case.
                if (view.containsKey(sid)) {
                    view.get(sid).recreateSocketAddresses();
                }
            }
        } else {
            LOG.debug("There is a connection already for server " + sid);
        }
    }
    
    
    /**
     * Try to establish a connection with each server if one
     * doesn't exist.
     */
    // 把所有需要发送消息的机器sid都连接上
    // 领导者选举的时候会用上
    public void connectAll(){
        long sid;
        // 连接所有queueSendMap记录的sid
        for(Enumeration<Long> en = queueSendMap.keys();
            en.hasMoreElements();){
            sid = en.nextElement();  // 服务器id
            connectOne(sid);
        }      
    }
    

    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    // 只要有一个发送队列是空的，则返回true?
    boolean haveDelivered() {
        //如果有一个队列是空的，代表交付过了？
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();
        
        softHalt();

        // clear data structures used for auth
        if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }
   
    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish();
        }
    }

    /**
     * Helper method to set socket options.
     * 
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(tcpKeepAlive);
        sock.setSoTimeout(socketTimeout);
    }

    /**
     * Helper method to close a socket.
     * 
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return number of connection processing threads.
     */
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    /**
     * Reset the value of connection processing threads count to zero.
     */
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    /**
     * Thread to listen on some port
     */
    //Listener主要监听本机配置的electionPort，不断的接收外部连接
    public class Listener extends ZooKeeperThread {

        volatile ServerSocket ss = null;

        public Listener() {
            // During startup of thread, thread name will be overridden to
            // specific election address
            super("ListenerThread");
        }

        /**
         * Sleeps on accept().
         */
        @Override
        public void run() {
            int numRetries = 0;
            InetSocketAddress addr;
            while((!shutdown) && (numRetries < 3)){
                try {
                    ss = new ServerSocket();
                    ss.setReuseAddress(true);
                    if (listenOnAllIPs) {
                        int port = view.get(QuorumCnxManager.this.mySid).electionAddr.getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                        addr = view.get(QuorumCnxManager.this.mySid).electionAddr;
                    }
                    LOG.info("My election bind port: " + addr.toString());
                    setName(view.get(QuorumCnxManager.this.mySid).electionAddr.toString());
                    ss.bind(addr);
                    while (!shutdown) {
                        Socket client = ss.accept();
                        setSockOpts(client);
                        LOG.info("Received connection request "
                                + client.getRemoteSocketAddress());

                        // Receive and handle the connection request
                        // asynchronously if the quorum sasl authentication is
                        // enabled. This is required because sasl server
                        // authentication process may take few seconds to finish,
                        // this may delay next peer connection requests.
                        if (quorumSaslAuthEnabled) {
                            receiveConnectionAsync(client);
                        } else {
                            // 不断接受连接,一个socket调用一次
                            receiveConnection(client);
                        }

                        numRetries = 0;
                    }
                } catch (IOException e) {
                    LOG.error("Exception while listening", e);
                    numRetries++;
                    try {
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                                  "Ignoring exception", ie);
                    }
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread, "
                        + "I won't be able to participate in leader "
                        + "election any longer: "
                        + view.get(QuorumCnxManager.this.mySid).electionAddr);
            }
        }
        
        /**
         * Halts this listener thread.
         */
        void halt(){
            try{
                LOG.debug("Trying to close listener: " + ss);
                if(ss != null) {
                    LOG.debug("Closing listener: "
                              + QuorumCnxManager.this.mySid);
                    ss.close();
                }
            } catch (IOException e){
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    // SendWorker类作为网络IO的发送者，从发送队列取出，发给对应sid的机器
    // 这个类作为“发送者”，继承ZooKeeperThread，线程不断地从发送队列取出，发给对应sid的机器属性
    // SendWorker和RecvWorker为什么要相互记录对方？
    // --- 原因就是从senderWorkerMap里根据sid找到SendWorker，然后方便调用finish方法
    class SendWorker extends ZooKeeperThread {
        // 目标机器sid，不是当前机器sid
        Long sid;
        Socket sock;
        // 记录RecvWorker
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         * 
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        //构造函数
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         * 
         * @return RecvWorker 
         */
        synchronized RecvWorker getRecvWorker(){
            return recvWorker;
        }
                
        synchronized boolean finish() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling finish for " + sid);
            }
            
            if(!running){
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }
            
            running = false;
            closeSocket(sock);
            // channel = null;

            this.interrupt();
            //调用recvWorker的finish
            if (recvWorker != null) {
                recvWorker.finish();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing entry from senderWorkerMap sid=" + sid);
            }
            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }

        // 往目标服务器发数据,消息在QuorumCnxManager#toSend中产生
        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            //线程数+1
            threadCnt.incrementAndGet();

            /**
             * 在SendWorker启动的时候，一旦Zookeeper发现针对当前服务器的消息发送队列为空，
             * 那么此时需要从lastMessageSent中取出一个最近发送过的消息来进行再次发送，
             * 这是为了解决接收方在消息接收前或者接收到消息后服务器挂了，导致消息尚未被正确处理。
             * 同时，Zookeeper能够保证接收方在处理消息时，会对重复消息进行正确的处理。
             */
            // SendWorker启动时发送队列没有东西的时候，把最后一次发送的内容再发一遍
            // 这样能解决 ”接收方在消息接收前或者接收到消息后服务器挂了“ 的问题
            // 那么倒数第二条为什么不发，倒数第三条为什么不发？？？
            try {
                /**
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                // 找到sid对应需要send的队列
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || isSendQueueEmpty(bq)) {
                    //如果没有什么发的，就把上一次发的再发一遍(重发能够正确处理)
                    ByteBuffer b = lastMessageSent.get(sid);
                   if (b != null) {
                       LOG.debug("Attempting to send lastMessage to sid=" + sid);
                       send(b);//发送
                   }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }

            // for循环
            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                        if (bq != null) {
                            // 从发送队列里面取出消息
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            // 队列没有记录在map中
                            LOG.error("No queue of incoming messages for " +"server " + sid);
                            break;
                        }

                        if(b != null){
                            // 更新最后一次发送的
                            lastMessageSent.put(sid, b);
                            // 发送
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid
                         + " my id = " + QuorumCnxManager.this.mySid
                         + " error = " + e);
            }
            this.finish();
            LOG.warn("Send worker leaving thread");
        }
    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    //作为网络IO的接受者，类似SendWorker，继承ZooKeeperThread，线程不断地从网络IO中读取数据，放入接收队列
    class RecvWorker extends ZooKeeperThread {
        // 来源方sid
        Long sid;
        Socket sock;
        volatile boolean running = true;
        final DataInputStream din;
        //记录SendWorker
        final SendWorker sw;


        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }
        
        /**
         * Shuts down this worker
         * 
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if(!running){
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }
            running = false;            

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            // 总线程数+1
            threadCnt.incrementAndGet();
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    // 获取长度
                    int length = din.readInt();
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    // 解析出byteBuffer
                    ByteBuffer message = ByteBuffer.wrap(msgArray);
                    // 加入接收队列
                    addToRecvQueue(new Message(message.duplicate(), sid));
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = "
                         + QuorumCnxManager.this.mySid + ", error = " , e);
            } finally {
                LOG.warn("Interrupting SendWorker");
                sw.finish();
                if (sock != null) {
                    closeSocket(sock);
                }
            }
        }
    }

    /**
     * Inserts an element in the specified queue. If the Queue is full, this
     * method removes an element from the head of the Queue and then inserts
     * the element at the tail. It can happen that the an element is removed
     * by another thread in {@link SendWorker#processMessage() processMessage}
     * method before this method attempts to remove an element from the queue.
     * This will cause {@link ArrayBlockingQueue#remove() remove} to throw an
     * exception, which is safe to ignore.
     *
     * Unlike {@link #addToRecvQueue(Message) addToRecvQueue} this method does
     * not need to be synchronized since there is only one thread that inserts
     * an element in the queue and another thread that reads from the queue.
     *
     * @param queue
     *          Reference to the Queue
     * @param buffer
     *          Reference to the buffer to be inserted in the queue
     */
    // 生成，加入sender队列
    // 每个sender的队列长度都是1,为了避免发送旧的数据，因此会把旧的remove掉
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,ByteBuffer buffer) {
        // 发送队列长度为1，如果满了就remove，然后add
        // 因为是选举leader投票，有特殊的要求:如果之前的票还没有投出去又产生了新的票，那么旧的票就可以直接作废了,不用真正的投出去
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    //消费sender队列
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          long timeout, TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts
     * the element at the tail of the queue.
     *
     * This method is synchronized to achieve fairness between two threads that
     * are trying to insert an element in the queue. Each thread checks if the
     * queue is full, then removes the element at the head of the queue, and
     * then inserts an element at the tail. This three-step process is done to
     * prevent a thread from blocking while inserting an element in the queue.
     * If we do not synchronize the call to this method, then a thread can grab
     * a slot in the queue created by the second thread. This can cause the call
     * to insert by the second thread to fail.
     * Note that synchronizing this method does not block another thread
     * from polling the queue since that synchronization is provided by the
     * queue itself.
     *
     * @param msg
     *          Reference to the message to be inserted in the queue
     */
    //加入接收队列
    public void addToRecvQueue(Message msg) {
        //接收队列满的时候就把最前面的删掉
        synchronized(recvQLock) {
            if (recvQueue.remainingCapacity() == 0) {
                try {
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                    // element could be removed by poll()
                     LOG.debug("Trying to remove from an empty " +
                         "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                //加入接收队列
                recvQueue.add(msg);
            } catch (IllegalStateException ie) {
                // This should never happen
                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    //消费接受队列,选主线程会调用
    public Message pollRecvQueue(long timeout, TimeUnit unit)
       throws InterruptedException {
       return recvQueue.poll(timeout, unit);
    }

    //是否连上了目标服务器
    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }
}
