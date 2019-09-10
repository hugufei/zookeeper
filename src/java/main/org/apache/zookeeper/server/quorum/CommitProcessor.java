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

import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 */
// 事务提交处理器。
// 1) 对于非事务请求，该处理器会直接将其交付给下一级处理器处理；
// 2) 对于事务请求，其会等待集群内针对Proposal的投票直到该Proposal可被提交，
//
// 利用CommitProcessor，每个服务器都可以很好地控制对事务请求的顺序处理。【单线程处理？？】
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /**
     * Requests that we are holding until the commit comes in.
     */
    // 请求队列，包含事务请求和非事务请求
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    // 已提交队列？？？
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    // 下一个处理器
    RequestProcessor nextProcessor;

    // 待处理的队列？？？
    ArrayList<Request> toProcess = new ArrayList<Request>();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    // 在leader端是false，learner端是true，因为learner端sync请求需要等待leader回复，而leader端本身则不需要
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
            boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    volatile boolean finished = false;

    // 对于非事务请求，直接调用nextProcessor， 对于事务请求，会阻塞，直到投票成功
    @Override
    public void run() {
        try {
            //下一个未处理的事务请求(不含leader端的sync请求),只要为null，都会while循环从queuedRequests里面找到第一个事务请求，或者直到队列为空
            Request nextPending = null;
            // 只要没有shutdown
            while (!finished) {
                int len = toProcess.size();
                // 待处理队列交给下个处理器,按顺序处理
                for (int i = 0; i < len; i++) {
                    // 非事务请求，或已经提交的事务请求，交给下一个处理器处理
                    nextProcessor.processRequest(toProcess.get(i));
                }
                // 队列清空
                toProcess.clear();

                // 注意这里的上锁，不会出现执行到过程中，queuedRequests的size变了
                synchronized (this) {
                    // 这部分结合尾部的while来读，要么 请求队列remove干净，要么从中找到一个事务请求，赋值给nextPending, 不允许size>0且nextPending == null的情况
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            //且 没有已提交事务
                            && committedRequests.size() == 0) {
                        // 一旦有事务请求，需要等待投票，这里还有一个功能就是，保证了请求的有序性
                        wait();
                        continue;
                    }
                    // First check and see if the commit came in for the pending
                    // request
                    // 不允许size>0且nextPending == null的情况
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            // 如果有 已提交的请求
                            && committedRequests.size() > 0) {
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        //如果Request和nextPending匹配
                        if (nextPending != null
                                && nextPending.sessionId == r.sessionId
                                && nextPending.cxid == r.cxid) {
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            // 加入待处理队列
                            toProcess.add(nextPending);
                            // 下一个pend的请求清空
                            nextPending = null;
                        } else {
                            // this request came from someone else so just
                            // send the commit packet
                            // 这种情况是nextPending还没有来的及设置，nextPending==null的情况(代码应该再细分一下if else),不可能出现nextPending!=null而走到了这里的情况(算异常)
                            toProcess.add(r);
                        }
                    }
                }

                // We haven't matched the pending requests, so go back to
                // waiting
                // 如果还有 未处理的事务请求(不含leader端的sync请求),就continue
                if (nextPending != null) {
                    continue;
                }

                //这一段的目的是找到一个 给nextPending赋值
                synchronized (this) {
                    // Process the next requests in the queuedRequests
                    // 只要queuedRequests队列不空，从中找到第一个 事务请求(不含leader端的sync请求),前面的其他请求全部加入待处理队列
                    while (nextPending == null && queuedRequests.size() > 0) {
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                        case OpCode.create:
                        case OpCode.delete:
                        case OpCode.setData:
                        case OpCode.multi:
                        case OpCode.setACL:
                        case OpCode.createSession:
                        case OpCode.closeSession:
                            //大部分事务请求直接赋给nextPending，然后break
                            nextPending = request;
                            break;
                        case OpCode.sync:
                            //如果需要等leader返回,该值learner端为true
                            if (matchSyncs) {
                                nextPending = request;
                            } else {
                                //不需要的话，直接加入待处理队列里
                                toProcess.add(request);
                            }
                            //leader端matchSyncs是false，learner端才需要等leader回复，这里也break
                            break;
                        default:
                            // 非事务请求，都直接加入待处理队列
                            toProcess.add(request);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    // 事务请求提交
    synchronized public void commit(Request request) {
        // 只要没有结束
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            // 进入已提交队列
            committedRequests.add(request);
            notifyAll();
        }
    }

    //加入queuedRequests
    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        if (!finished) {
            // 生产到请求队列
            queuedRequests.add(request);
            notifyAll();
        }
    }

    // 关闭
    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
