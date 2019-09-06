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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

/**
 * Interface to a Server connection - represents a connection from a client
 * to the server.
 */
// ServerCnxn 是一个 ZooKeeper 客户端和服务器之间的连接接口，代表了一个客户端和服务器的连接.
// 实现了Watcher接口，有两个子类
// 作用： 这个Watcher的实现类记录了client和server的连接，回调的时候，可以直接发送response告诉client，有事件触发了
public abstract class ServerCnxn implements Stats, Watcher {
    // This is just an arbitrary object to represent requests issued by
    // (aka owned by) this class

    // 代表由本类提出的请求
    final public static Object me = new Object();

    // 认证信息
    protected ArrayList<Id> authInfo = new ArrayList<Id>();

    /**
     * If the client is of old version, we don't send r-o mode info to it.
     * The reason is that if we would, old C client doesn't read it, which
     * results in TCP RST packet, i.e. "connection reset by peer".
     */
    // 是否为旧的C客户端
    boolean isOldClient = true;

    // 获取会话超时时间
    abstract int getSessionTimeout();

    // 关闭
    abstract void close();

    // 发送响应
    public abstract void sendResponse(ReplyHeader h, Record r, String tag) throws IOException;

    /* notify the client the session is closing and close/cleanup socket */
    // 关闭会话
    abstract void sendCloseSession();

    // 处理，Watcher接口中的方法
    public abstract void process(WatchedEvent event);

    // 获取会话id
    abstract long getSessionId();

    // 设置会话id
    abstract void setSessionId(long sessionId);

    /** auth info for the cnxn, returns an unmodifyable list */
    // 获取认证信息，返回不可修改的列表
    public List<Id> getAuthInfo() {
        return Collections.unmodifiableList(authInfo);
    }

    // 添加认证信息
    public void addAuthInfo(Id id) {
        if (authInfo.contains(id) == false) {
            authInfo.add(id);
        }
    }
    // 移除认证信息
    public boolean removeAuthInfo(Id id) {
        return authInfo.remove(id);
    }

    // 发送数据
    abstract void sendBuffer(ByteBuffer closeConn);

    // 允许接收
    abstract void enableRecv();

    // 不允许接收
    abstract void disableRecv();

    // 设置会话超时时间
    abstract void setSessionTimeout(int sessionTimeout);

    /**
     * Wrapper method to return the socket address
     */
    public abstract InetAddress getSocketAddress();

    // Zookeeper的Sasl服务器
    protected ZooKeeperSaslServer zooKeeperSaslServer = null;

    // 请求关闭异常类
    protected static class CloseRequestException extends IOException {
        private static final long serialVersionUID = -7854505709816442681L;

        public CloseRequestException(String msg) {
            super(msg);
        }
    }

    // 流结束异常类
    protected static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -8255690282104294178L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    // CMD命令
    protected final static int confCmd =
        ByteBuffer.wrap("conf".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int consCmd =
        ByteBuffer.wrap("cons".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int crstCmd =
        ByteBuffer.wrap("crst".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int dumpCmd =
        ByteBuffer.wrap("dump".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int enviCmd =
        ByteBuffer.wrap("envi".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int getTraceMaskCmd =
        ByteBuffer.wrap("gtmk".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int ruokCmd =
        ByteBuffer.wrap("ruok".getBytes()).getInt();
    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int setTraceMaskCmd =
        ByteBuffer.wrap("stmk".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int srvrCmd =
        ByteBuffer.wrap("srvr".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int srstCmd =
        ByteBuffer.wrap("srst".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int statCmd =
        ByteBuffer.wrap("stat".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int wchcCmd =
        ByteBuffer.wrap("wchc".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int wchpCmd =
        ByteBuffer.wrap("wchp".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int wchsCmd =
        ByteBuffer.wrap("wchs".getBytes()).getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int mntrCmd = ByteBuffer.wrap("mntr".getBytes())
            .getInt();

    /*
     * See <a href="{@docRoot}/../../../docs/zookeeperAdmin.html#sc_zkCommands">
     * Zk Admin</a>. this link is for all the commands.
     */
    protected final static int isroCmd = ByteBuffer.wrap("isro".getBytes())
            .getInt();

    //存储CMD的整形值与String的键值对
    final static Map<Integer, String> cmd2String = new HashMap<Integer, String>();

    private static final String ZOOKEEPER_4LW_COMMANDS_WHITELIST = "zookeeper.4lw.commands.whitelist";

    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxn.class);

    private static final Set<String> whiteListedCommands = new HashSet<String>();

    private static boolean whiteListInitialized = false;

    // @VisibleForTesting
    public synchronized static void resetWhiteList() {
        whiteListInitialized = false;
        whiteListedCommands.clear();
    }

    /**
     * Return the string representation of the specified command code.
     */
    public static String getCommandString(int command) {
        return cmd2String.get(command);
    }

    /**
     * Check if the specified command code is from a known command.
     *
     * @param command The integer code of command.
     * @return true if the specified command is known, false otherwise.
     */
    public static boolean isKnown(int command) {
        return cmd2String.containsKey(command);
    }

    /**
     * Check if the specified command is enabled.
     *
     * In ZOOKEEPER-2693 we introduce a configuration option to only
     * allow a specific set of white listed commands to execute.
     * A command will only be executed if it is also configured
     * in the white list.
     *
     * @param command The command string.
     * @return true if the specified command is enabled.
     */
    public synchronized static boolean isEnabled(String command) {
        if (whiteListInitialized) {
            return whiteListedCommands.contains(command);
        }

        String commands = System.getProperty(ZOOKEEPER_4LW_COMMANDS_WHITELIST);
        if (commands != null) {
            String[] list = commands.split(",");
            for (String cmd : list) {
                if (cmd.trim().equals("*")) {
                    for (Map.Entry<Integer, String> entry : cmd2String.entrySet()) {
                        whiteListedCommands.add(entry.getValue());
                    }
                    break;
                }
                if (!cmd.trim().isEmpty()) {
                    whiteListedCommands.add(cmd.trim());
                }
            }
        } else {
            for (Map.Entry<Integer, String> entry : cmd2String.entrySet()) {
                String cmd = entry.getValue();
                if (cmd.equals("wchc") || cmd.equals("wchp")) {
                    // ZOOKEEPER-2693 / disable these exploitable commands by default.
                    continue;
                }
                whiteListedCommands.add(cmd);
            }
        }

        // Readonly mode depends on "isro".
        if (System.getProperty("readonlymode.enabled", "false").equals("true")) {
            whiteListedCommands.add("isro");
        }
        // zkServer.sh depends on "srvr".
        whiteListedCommands.add("srvr");
        whiteListInitialized = true;
        LOG.info("The list of known four letter word commands is : {}", Collections.singletonList(cmd2String));
        LOG.info("The list of enabled four letter word commands is : {}", Collections.singletonList(whiteListedCommands));
        return whiteListedCommands.contains(command);
    }

    // specify all of the commands that are available
    static {
        cmd2String.put(confCmd, "conf");
        cmd2String.put(consCmd, "cons");
        cmd2String.put(crstCmd, "crst");
        cmd2String.put(dumpCmd, "dump");
        cmd2String.put(enviCmd, "envi");
        cmd2String.put(getTraceMaskCmd, "gtmk");
        cmd2String.put(ruokCmd, "ruok");
        cmd2String.put(setTraceMaskCmd, "stmk");
        cmd2String.put(srstCmd, "srst");
        cmd2String.put(srvrCmd, "srvr");
        cmd2String.put(statCmd, "stat");
        cmd2String.put(wchcCmd, "wchc");
        cmd2String.put(wchpCmd, "wchp");
        cmd2String.put(wchsCmd, "wchs");
        cmd2String.put(mntrCmd, "mntr");
        cmd2String.put(isroCmd, "isro");
    }

    // 接收packet时，改变统计状态
    protected void packetReceived() {
        incrPacketsReceived();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsReceived();
        }
    }

    // 发送packet时，改变统计状态
    protected void packetSent() {
        incrPacketsSent();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsSent();
        }
    }

    //获取服务器的统计数据
    protected abstract ServerStats serverStats();

    /**
     * 服务器的统计数据
     **/
    // 创建连接的时间
    protected final Date established = new Date();

    // 接受的packet数量
    protected final AtomicLong packetsReceived = new AtomicLong();
    // 发送的packet数量
    protected final AtomicLong packetsSent = new AtomicLong();

    // 最小延迟
    protected long minLatency;
    // 最大延迟
    protected long maxLatency;
    // 最后操作类型
    protected String lastOp;
    // 最后的cxid???cxid是什么?? 应该是client xid
    protected long lastCxid;
    // 最后的zxid
    protected long lastZxid;
    // 最后的响应时间
    protected long lastResponseTime;
    // 最后的延迟
    protected long lastLatency;
    // 数量
    protected long count;
    // 总的延迟
    protected long totalLatency;

    //还原各种计数器
    public synchronized void resetStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        minLatency = Long.MAX_VALUE;
        maxLatency = 0;
        lastOp = "NA";
        lastCxid = -1;
        lastZxid = -1;
        lastResponseTime = 0;
        lastLatency = 0;

        count = 0;
        totalLatency = 0;
    }

    // 增加接收的packet数量
    protected long incrPacketsReceived() {
        return packetsReceived.incrementAndGet();
    }

    protected void incrOutstandingRequests(RequestHeader h) {
    }

    // 增加发送的packet数量
    protected long incrPacketsSent() {
        return packetsSent.incrementAndGet();
    }

    // 更新响应的统计数据
    protected synchronized void updateStatsForResponse(long cxid, long zxid,
            String op, long start, long end)
    {
        // don't overwrite with "special" xids - we're interested
        // in the clients last real operation
        if (cxid >= 0) {
            lastCxid = cxid;
        }
        lastZxid = zxid;
        lastOp = op;
        lastResponseTime = end;
        long elapsed = end - start;
        lastLatency = elapsed;
        if (elapsed < minLatency) {
            minLatency = elapsed;
        }
        if (elapsed > maxLatency) {
            maxLatency = elapsed;
        }
        count++;
        totalLatency += elapsed;
    }

    //获取建立连接的时间
    public Date getEstablished() {
        return (Date)established.clone();
    }

    //获取已经提交但是尚未回复的请求个数
    public abstract long getOutstandingRequests();

    //获取接收到的packets个数
    public long getPacketsReceived() {
        return packetsReceived.longValue();
    }

    //获取已经发送packet个数
    public long getPacketsSent() {
        return packetsSent.longValue();
    }

    //最低延迟
    public synchronized long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    //平均延时
    public synchronized long getAvgLatency() {
        return count == 0 ? 0 : totalLatency / count;
    }

    // 最高延迟
    public synchronized long getMaxLatency() {
        return maxLatency;
    }

    //连接最后一次操作
    public synchronized String getLastOperation() {
        return lastOp;
    }

    //连接最后的cxid
    public synchronized long getLastCxid() {
        return lastCxid;
    }

    //连接最后的zxid
    public synchronized long getLastZxid() {
        return lastZxid;
    }

    //上次回复的时间
    public synchronized long getLastResponseTime() {
        return lastResponseTime;
    }

    //上一次回复的延迟
    public synchronized long getLastLatency() {
        return lastLatency;
    }

    /**
     * Prints detailed stats information for the connection.
     *
     * @see dumpConnectionInfo(PrintWriter, boolean) for brief stats
     */
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpConnectionInfo(pwriter, false);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    public abstract InetSocketAddress getRemoteSocketAddress();
    public abstract int getInterestOps();
    
    /**
     * Print information about the connection.
     * @param brief iff true prints brief details, otw full detail
     * @return information about this connection
     */
    protected synchronized void
    dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        pwriter.print(" ");
        pwriter.print(getRemoteSocketAddress());
        pwriter.print("[");
        int interestOps = getInterestOps();
        pwriter.print(interestOps == 0 ? "0" : Integer.toHexString(interestOps));
        pwriter.print("](queued=");
        pwriter.print(getOutstandingRequests());
        pwriter.print(",recved=");
        pwriter.print(getPacketsReceived());
        pwriter.print(",sent=");
        pwriter.print(getPacketsSent());

        if (!brief) {
            long sessionId = getSessionId();
            if (sessionId != 0) {
                pwriter.print(",sid=0x");
                pwriter.print(Long.toHexString(sessionId));
                pwriter.print(",lop=");
                pwriter.print(getLastOperation());
                pwriter.print(",est=");
                pwriter.print(getEstablished().getTime());
                pwriter.print(",to=");
                pwriter.print(getSessionTimeout());
                long lastCxid = getLastCxid();
                if (lastCxid >= 0) {
                    pwriter.print(",lcxid=0x");
                    pwriter.print(Long.toHexString(lastCxid));
                }
                pwriter.print(",lzxid=0x");
                pwriter.print(Long.toHexString(getLastZxid()));
                pwriter.print(",lresp=");
                pwriter.print(getLastResponseTime());
                pwriter.print(",llat=");
                pwriter.print(getLastLatency());
                pwriter.print(",minlat=");
                pwriter.print(getMinLatency());
                pwriter.print(",avglat=");
                pwriter.print(getAvgLatency());
                pwriter.print(",maxlat=");
                pwriter.print(getMaxLatency());
            }
        }
        pwriter.print(")");
    }

}
