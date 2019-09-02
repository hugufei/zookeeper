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
import java.util.LinkedList;
import java.util.List;

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ClientCnxnSocket does the lower level communication with a socket
 * implementation.
 * 
 * This code has been moved out of ClientCnxn so that a Netty implementation can
 * be provided as an alternative to the NIO socket code.
 * 
 */
// ClientCnxnSocket定义了底层Socket通信的接口.默认实现是ClientCnxnSocketNIO.
abstract class ClientCnxnSocket {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocket.class);

    protected boolean initialized; // 是否初始化

    /**
     * This buffer is only used to read the length of the incoming message.
     */
    // 仅仅用来读取 incoming message的长度
    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);

    /**
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;

    // send次数
    protected long sentCount = 0;
    // 接收次数
    protected long recvCount = 0;
    // 上次接收时间
    protected long lastHeard;
    // 上次发送时间
    protected long lastSend;
    // 当前时间
    protected long now;
    // 客户端通信的发送线程
    protected ClientCnxn.SendThread sendThread;

    /**
     * The sessionId is only available here for Log and Exception messages.
     * Otherwise the socket doesn't need to know it.
     */
    // 仅仅用来辅助log和Exception记录用的
    protected long sessionId;

    // 设置sendThread以及sessionId
    void introduce(ClientCnxn.SendThread sendThread, long sessionId) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
    }

    void updateNow() {
        now = Time.currentElapsedTime();
    }

    // 获取接收的闲置时间
    int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    // 获取发送的闲置时间
    int getIdleSend() {
        return (int) (now - lastSend);
    }

    long getSentCount() {
        return sentCount;
    }

    // 接收次数
    long getRecvCount() {
        return recvCount;
    }

    // 更新最后一次监听的时间
    void updateLastHeard() {
        this.lastHeard = now;
    }

    // 更新最后一次发送的时间
    void updateLastSend() {
        this.lastSend = now;
    }

    // 同时更新最后一次监听和发送的时间
    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    // 读取incoming message的length
    protected void readLength() throws IOException {
        int len = incomingBuffer.getInt();
        //默认长度[0,4M]之间
        if (len < 0 || len >= ClientCnxn.packetLen) {
            throw new IOException("Packet len" + len + " is out of range!");
        }
        //分配对应长度的空间
        incomingBuffer = ByteBuffer.allocate(len);
    }

    // 读取connect的response
    void readConnectResult() throws IOException {
        if (LOG.isTraceEnabled()) {
            StringBuilder buf = new StringBuilder("0x[");
            for (byte b : incomingBuffer.array()) {
                buf.append(Integer.toHexString(b) + ",");
            }
            buf.append("]");
            LOG.trace("readConnectResult " + incomingBuffer.remaining() + " "
                    + buf.toString());
        }
        ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        //反序列化出 ConnectResponse结果
        ConnectResponse conRsp = new ConnectResponse();
        conRsp.deserialize(bbia, "connect");

        // read "is read-only" flag
        // 反序列化,看是否是只读的
        boolean isRO = false;
        try {
            isRO = bbia.readBool("readOnly");
        } catch (IOException e) {
            // this is ok -- just a packet from an old server which
            // doesn't contain readOnly field
            LOG.warn("Connected to an old server; r-o mode will be unavailable");
        }
        this.sessionId = conRsp.getSessionId();
        // 连接成功后，sendThread完成connect时一些参数验证以及zk state更新以及事件处理
        sendThread.onConnected(conRsp.getTimeOut(), this.sessionId, conRsp.getPasswd(), isRO);
    }

    abstract boolean isConnected();

    abstract void connect(InetSocketAddress addr) throws IOException;

    abstract SocketAddress getRemoteSocketAddress();

    abstract SocketAddress getLocalSocketAddress();

    abstract void cleanup();

    abstract void close();

    abstract void wakeupCnxn();

    abstract void enableWrite();

    abstract void disableWrite();

    abstract void enableReadWriteOnly();

    abstract void doTransport(int waitTimeOut, List<Packet> pendingQueue,
            LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException;

    abstract void testableCloseSocket() throws IOException;

    abstract void sendPacket(Packet p) throws IOException;
}
