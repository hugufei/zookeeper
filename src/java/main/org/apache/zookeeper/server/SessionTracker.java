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

import java.io.PrintWriter;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;

/**
 * This is the basic interface that ZooKeeperServer uses to track sessions. The
 * standalone and leader ZooKeeperServer use the same SessionTracker. The
 * FollowerZooKeeperServer uses a SessionTracker which is basically a simple
 * shell to track information to be forwarded to the leader.
 *
 * Zookeeper使用SessionTracker来负责对于会话的超时检查。
 * SessionTracker使用单独的线程（超时检查线程）专门进行会话超时检查，即逐个依次地对会话桶中剩下的会话进行清理。
 * 如果一个会话被激活，那么Zookeeper就会将其从上一个会话桶迁移到下一个会话桶中：
 * 如ExpirationTime 1 的session n 迁移到ExpirationTime n 中，此时ExpirationTime 1中留下的所有会话都是尚未被激活的。
 * 超时检查线程就定时检查这个会话桶中所有剩下的未被迁移的会话。
 * 超时检查线程只需要在这些指定时间点（ExpirationTime 1、ExpirationTime 2...）上进行检查即可，这样提高了检查的效率，性能也非常好。
 *
 */
public interface SessionTracker {
    public static interface Session {
        long getSessionId();
        int getTimeout();
        boolean isClosing();
    }
    public static interface SessionExpirer {

        //过期某个session
        void expire(Session session);

        //获取serverId
        long getServerId();
    }

    //创建session
    long createSession(int sessionTimeout);

    //添加session
    void addSession(long id, int to);

    /**
     * @param sessionId
     * @param sessionTimeout
     * @return false if session is no longer active
     *
     */
    //会话激活,当会话失效的时候返回false
    boolean touchSession(long sessionId, int sessionTimeout);

    /**
     * Mark that the session is in the process of closing.
     * @param sessionId
     */
    //标志会话为关闭中
    void setSessionClosing(long sessionId);

    // 关闭当前会话检测的线程
    void shutdown();

    /**
     * @param sessionId
     */
    //移除会话信息
    void removeSession(long sessionId);

    //会话检测
    void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, SessionMovedException;

    //设置会话的拥有者
    void setOwner(long id, Object owner) throws SessionExpiredException;

    /**
     * Text dump of session information, suitable for debugging.
     * @param pwriter the output writer
     */
    //将会话dump下来
    void dumpSessions(PrintWriter pwriter);
}
