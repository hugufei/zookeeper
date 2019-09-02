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

package org.apache.zookeeper.server.persistence;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 *
 */
// 接口类型，读取事务性日志的接口。
// zk事务日志文件用来记录事物操作，每一个事务操作如添加，删除节点等等，都会在事务日志中记录一条记录，用来在zookeeper异常情况下恢复数据
// 正常运行过程中，针对所有更新操作，在返回客户端“更新成功”的响应前，ZK会确保已经将本次更新操作的事务日志写到磁盘上，只有这样，整个更新操作才会生效。
public interface TxnLog {

    /**
     +     * Setter for ServerStats to monitor fsync threshold exceed
     +     * @param serverStats used to update fsyncThresholdExceedCount
     +     */
     void setServerStats(ServerStats serverStats);

    /**
     * roll the current
     * log being appended to
     * @throws IOException 
     */
    // 滚动日志，从当前日志滚到下一个日志，不是回滚
    void rollLog() throws IOException;
    /**
     * Append a request to the transaction log
     * @param hdr the transaction header
     * @param r the transaction itself
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    // 添加一个请求至事务性日志
    boolean append(TxnHeader hdr, Record r) throws IOException;

    /**
     * Start reading the transaction logs
     * from a given zxid
     * @param zxid
     * @return returns an iterator to read the 
     * next transaction in the logs.
     * @throws IOException
     */
    // 读取事务性日志
    TxnIterator read(long zxid) throws IOException;
    
    /**
     * the last zxid of the logged transactions.
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    // 事务性操作的最新zxid
    long getLastLoggedZxid() throws IOException;
    
    /**
     * truncate the log to get in sync with the 
     * leader.
     * @param zxid the zxid to truncate at.
     * @throws IOException 
     */
    // 清空zxid以后的日志
    boolean truncate(long zxid) throws IOException;
    
    /**
     * the dbid for this transaction log. 
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    // 获取数据库的id
    long getDbId() throws IOException;
    
    /**
     * commmit the trasaction and make sure
     * they are persisted
     * @throws IOException
     */
    // 提交事务并进行确认
    void commit() throws IOException;
   
    /** 
     * close the transactions logs
     */
    // 关闭事务性日志
    void close() throws IOException;
    /**
     * an iterating interface for reading 
     * transaction logs. 
     */

    // 读取事务日志的迭代器接口
    public interface TxnIterator {
        /**
         * return the transaction header.
         * @return return the transaction header.
         */
        // 获取事务头部
        TxnHeader getHeader();
        
        /**
         * return the transaction record.
         * @return return the transaction record.
         */
        // 获取事务
        Record getTxn();
     
        /**
         * go to the next transaction record.
         * @throws IOException
         */
        // 下个事务
        boolean next() throws IOException;
        
        /**
         * close files and release the 
         * resources
         * @throws IOException
         */
        // 关闭文件释放资源
        void close() throws IOException;
    }
}

