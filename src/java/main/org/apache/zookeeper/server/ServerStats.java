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



import org.apache.zookeeper.common.Time;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic Server Statistics
 *
 * 服务器状态类
 */
public class ServerStats {
    // 发送包个数
    private long packetsSent;
    // 接收个数
    private long packetsReceived;
    // 最长延迟
    private long maxLatency;
    // 最短延迟
    private long minLatency = Long.MAX_VALUE;
    // 总延迟
    private long totalLatency = 0;
    // 延迟次数
    private long count = 0;

    private AtomicLong fsyncThresholdExceedCount = new AtomicLong(0);

    // Provider对象，提供部分统计数据
    private final Provider provider;

    //内部类Provider完成服务器状态，队列数量，client存活数量等信息
    public interface Provider {
        // 获取队列中还没有被处理的请求数量
        public long getOutstandingRequests();
        // 获取最后一个处理的zxid
        public long getLastProcessedZxid();
        // 获取服务器状态
        // 有5种实现，用于区分单机，集群不同server的状态
        public String getState();
        // 获取存活的客户端连接总数
        public int getNumAliveConnections();
    }

    // 指provider来完成getOutstandingRequests,getLastProcessedZxid等调用
    // 可以看下Provider的实现类
    public ServerStats(Provider provider) {
        this.provider = provider;
    }
    
    // getters
    synchronized public long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    synchronized public long getAvgLatency() {
        if (count != 0) {
            return totalLatency / count;
        }
        return 0;
    }

    synchronized public long getMaxLatency() {
        return maxLatency;
    }

    public long getOutstandingRequests() {
        return provider.getOutstandingRequests();
    }
    
    public long getLastProcessedZxid(){
        return provider.getLastProcessedZxid();
    }
    
    synchronized public long getPacketsReceived() {
        return packetsReceived;
    }

    synchronized public long getPacketsSent() {
        return packetsSent;
    }

    public String getServerState() {
        return provider.getState();
    }
    
    /** The number of client connections alive to this server */
    public int getNumAliveClientConnections() {
    	return provider.getNumAliveConnections();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/"
                + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        sb.append("Connections: " + getNumAliveClientConnections() + "\n");

        if (provider != null) {
            sb.append("Outstanding: " + getOutstandingRequests() + "\n");
            sb.append("Zxid: 0x"+ Long.toHexString(getLastProcessedZxid())+ "\n");
        }
        sb.append("Mode: " + getServerState() + "\n");
        return sb.toString();
    }

    public long getFsyncThresholdExceedCount() {
        return fsyncThresholdExceedCount.get();
    }

    public void incrementFsyncThresholdExceedCount() {
        fsyncThresholdExceedCount.incrementAndGet();
    }

    public void resetFsyncThresholdExceedCount() {
        fsyncThresholdExceedCount.set(0);
    }

    // mutators
    // 更新延迟统计数据
    synchronized void updateLatency(long requestCreateTime) {
        long latency = Time.currentElapsedTime() - requestCreateTime;
        totalLatency += latency;
        count++;
        if (latency < minLatency) {
            minLatency = latency;
        }
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }

    // 重置延迟数据
    synchronized public void resetLatency(){
        totalLatency = 0;
        count = 0;
        maxLatency = 0;
        minLatency = Long.MAX_VALUE;
    }

    // 重置max延迟
    synchronized public void resetMaxLatency(){
        maxLatency = getMinLatency();
    }

    // 接受次数+1
    synchronized public void incrementPacketsReceived() {
        packetsReceived++;
    }

    // 发送次数+1
    synchronized public void incrementPacketsSent() {
        packetsSent++;
    }

    // 重置请求量统计
    synchronized public void resetRequestCounters(){
        packetsReceived = 0;
        packetsSent = 0;
    }


    synchronized public void reset() {
        resetLatency();
        resetRequestCounters();
    }

}
