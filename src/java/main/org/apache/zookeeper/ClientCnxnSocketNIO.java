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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//主要就是封装了NIO的东西
public class ClientCnxnSocketNIO extends ClientCnxnSocket {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocketNIO.class);

    private final Selector selector = Selector.open();

    private SelectionKey sockKey;

    ClientCnxnSocketNIO() throws IOException {
        super();
    }

    //这个只是说SelectionKey有没有初始化，来标示，并不是真正的Connected
    @Override
    boolean isConnected() {
        return sockKey != null;
    }

    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    // 利用pendingQueue和outgoingQueue进行IO
    void doIO(List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn) throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }

        // 若读就绪
        // 1) 没有初始化就完成初始化
        // 2) 读取len再给incomingBuffer分配对应空间
        // 3) 读取对应的response
        // 注意,读的时候是分两次读的 :
        // 第一次只读len，然后给incomingBuffer分配对应的空间 第二次再把剩下的内容读完
        if (sockKey.isReadable()) {
            // 第一遍的时候，incomingBuffer=lenBuffer，读的是长度
            // 第二遍的时候，incomingBuffer的空间大小就是真正的消息的空间大小
            int rc = sock.read(incomingBuffer);
            //如果<0,表示读到末尾了,这种情况出现在连接关闭的时候
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            // 如果incomingBuffer没有剩余空间了【即读满了】
            // 【第一次incomingBuffer为lenBuffer，目的是为了从sock中读取消息的长度len】
            // 【第二次incomingBuffer分配了对应len的空间，目的是为了从sock中读取真实的消息】
            if (!incomingBuffer.hasRemaining()) {
                // incomingBuffer切换到读模式。即处理从sock读到incomingBuffer的数据
                incomingBuffer.flip();
                // 第一遍的时候，incomingBuffer与lenBuffer相等
                if (incomingBuffer == lenBuffer) {
                    recvCount++; // 接收次数+1
                    readLength(); // 获取len并给incomingBuffer分配对应空间，然后再次走doIO方法
                } else if (!initialized) {  // 如果client和server的连接还没有初始化
                    readConnectResult(); // 读取connect 回复
                    enableRead(); //启用读
                    // 如果有可以发送的packet
                    if (findSendablePacket(outgoingQueue, cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite(); //当前socket允许写，因为有要发送的packet
                    }
                    lenBuffer.clear();
                    // 还原incomingBuffer
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;//client和server连接初始化完成
                } else {
                    // 如果已连接，并且已经给incomingBuffer分配了对应len的空间
                    sendThread.readResponse(incomingBuffer); //读取response
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer; //还原incomingBuffer
                    updateLastHeard();
                }
            }
        }

        // 若写就绪
        // 1) 找到可以发送的Packet
        // 2) 如果Packet的byteBuffer没有创建，那么就创建
        // 3) byteBuffer写入socketChannel
        // 4) 把Packet从outgoingQueue中取出来，放到pendingQueue中
        if (sockKey.isWritable()) {
            synchronized(outgoingQueue) {
                // 找到可以发送的Packet
                Packet p = findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress());

                if (p != null) {
                    updateLastSend();
                    // If we already started writing p, p.bb will already exist
                    // 如果packet还没有生成byteBuffer，那就生成byteBuffer
                    if (p.bb == null) {
                        if ((p.requestHeader != null) &&
                                (p.requestHeader.getType() != OpCode.ping) &&
                                (p.requestHeader.getType() != OpCode.auth)) {
                            //xid++
                            p.requestHeader.setXid(cnxn.getXid());
                        }
                        // 如果packet还没有生成byteBuffer，那就生成byteBuffer
                        p.createBB();
                    }
                    // 往socket中写数据，就是往服务端发送数据
                    sock.write(p.bb);
                    // 没有剩下的了
                    if (!p.bb.hasRemaining()) {
                        sentCount++;
                        // 从待发送队列中取出该packet
                        outgoingQueue.removeFirstOccurrence(p);
                        if (p.requestHeader != null
                                && p.requestHeader.getType() != OpCode.ping
                                && p.requestHeader.getType() != OpCode.auth) {
                            synchronized (pendingQueue) {
                                // 加入待回复的队列
                                pendingQueue.add(p);
                            }
                        }
                    }
                }
                // 如果没有要发的，就禁止写
                if (outgoingQueue.isEmpty()) {
                    // No more packets to send: turn off write interest flag.
                    // Will be turned on later by a later call to enableWrite(),
                    // from within ZooKeeperSaslClient (if client is configured
                    // to attempt SASL authentication), or in either doIO() or
                    // in doTransport() if not.
                    disableWrite();
                } else if (!initialized && p != null && !p.bb.hasRemaining()) {
                    // On initial connection, write the complete connect request
                    // packet, but then disable further writes until after
                    // receiving a successful connection response.  If the
                    // session is expired, then the server sends the expiration
                    // response and immediately closes its end of the socket.  If
                    // the client is simultaneously writing on its end, then the
                    // TCP stack may choose to abort with RST, in which case the
                    // client would never receive the session expired event.  See
                    // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                    disableWrite();
                } else {
                    // Just in case
                    enableWrite();
                }
            }
        }
    }

    //从outgoingQueue中找到可以发送的packet，有的话就是返回第一个
    //1) 如果没有要发送的就返回null
    //2) 如果有要发送的或者client没有在处理sasl的权限，那么就拿队列第一个
    //3) 如果在处理sasl，那么遍历队列，把没有requestHeader为null的放到队头，返回该packet
    private Packet findSendablePacket(LinkedList<Packet> outgoingQueue,
                                      boolean clientTunneledAuthenticationInProgress) {
        synchronized (outgoingQueue) {
            // 如果没有要发送的
            if (outgoingQueue.isEmpty()) {
                return null;
            }
            // 如果有要发送的 或者 没有在处理sasl的权限
            if (outgoingQueue.getFirst().bb != null // If we've already starting sending the first packet, we better finish
                || !clientTunneledAuthenticationInProgress) {
                return outgoingQueue.getFirst();
            }

            // Since client's authentication with server is in progress,
            // send only the null-header packet queued by primeConnection().
            // This packet must be sent so that the SASL authentication process
            // can proceed, but all other packets should wait until
            // SASL authentication completes.
            ListIterator<Packet> iter = outgoingQueue.listIterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                //如果在处理sasl的权限，那么只有requestHeader为null的Packet可以被发送
                if (p.requestHeader == null) {
                    // We've found the priming-packet. Move it to the beginning of the queue.
                    iter.remove();
                    outgoingQueue.add(0, p);
                    return p;
                } else {
                    // Non-priming packet: defer it until later, leaving it in the queue
                    // until authentication completes.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("deferring non-priming packet: " + p +
                                "until SASL authentication completes.");
                    }
                }
            }
            // no sendable packet found.
            return null;
        }
    }

    //socketChannel关闭，SelectionKey置空
    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }

    //selector关闭
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    // 创建SocketChannel
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);// 非阻塞
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) throws IOException {
        // 将socketChannel注册到selector上，并且监听连接事件
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        // registerAndConnect中如果立即connect就调用sendThread.primeConnection();
        boolean immediateConnect = sock.connect(addr);
        // 如果立即connect就调用sendThread.primeConnection().
        // 如果没有立即connect上，那么就在下面介绍的doTransport中等待SocketChannel finishConnect再调用
        if (immediateConnect) {
            // client把watches和authData等数据发过去，并更新SelectionKey为读写
            sendThread.primeConnection();
        }
    }
    
    @Override
    void connect(InetSocketAddress addr) throws IOException {
        // 建立socket
        SocketChannel sock = createSock();
        try {
            // 注册这个sock到服务端
           registerAndConnect(sock, addr);
        } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        //还没有初始化,connect ok了但是还读到server的response
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    //获取远端地址
    @Override
    SocketAddress getRemoteSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getRemoteSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Returns the local address to which the socket is bound.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    // 获取本地地址
    @Override
    SocketAddress getLocalSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getLocalSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    // 唤醒selector
    @Override
    synchronized void wakeupCnxn() {
        selector.wakeup();
    }
    // 1) 如果是连接就绪，调用sendThread连接操作
    // 2) 若读写就绪，调用doIO函数
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException {
        // 找到就绪的keys个数
        selector.select(waitTimeOut);
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        updateNow();
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            // 如果是连接就绪，调用sendThread连接操作
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) { // 如果就绪的是connect事件，这个出现在registerAndConnect函数没有立即连接成功
                // 如果完成了连接
                if (sc.finishConnect()) {
                    // 更新时间
                    updateLastSendAndHeard();
                    // client把watches和authData等数据发过去，并更新SelectionKey为读写
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {// 如果就绪的是读或者写事件
                // 利用pendingQueue和outgoingQueue进行IO
                doIO(pendingQueue, outgoingQueue, cnxn);
            }
        }
        // 如果zk的state是已连接
        if (sendThread.getZkState().isConnected()) {
            synchronized(outgoingQueue) {
                if (findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {  // 如果有可以发送的packet
                    //允许写
                    enableWrite();
                }
            }
        }
        // 清空
        selected.clear();
    }

    //TODO should this be synchronized?
    // 测试socket关闭
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        ((SocketChannel) sockKey.channel()).socket().close();
    }

    // 开启写【写Socket,就是允许发送数据】
    @Override
    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    // 禁止写
    @Override
    public synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    //开启读
    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    //仅允许读写
    @Override
    synchronized void enableReadWriteOnly() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    //发送packet
    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }


}
