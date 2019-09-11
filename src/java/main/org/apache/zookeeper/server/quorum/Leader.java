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

import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the control logic for the Leader.
 */
// Leader服务器是Zookeeper集群工作的核心，其主要工作如下
// (1) 事务请求的唯一调度和处理者，保证集群事务处理的顺序性。
// (2) 集群内部各服务器的调度者。
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    
    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    //提议
    static public class Proposal {
        // 集群间传递的包
        public QuorumPacket packet;

        //接收到ack的机器sid集合
        public HashSet<Long> ackSet = new HashSet<Long>();

        // 请求
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

    //LeaderZooKeeperServer
    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

    //是否已有过半参与者确认当前leader并且完成数据同步[针对Leader的NEWLEADER消息回复了Ack]
    protected boolean quorumFormed = false;
    
    // the follower acceptor thread
    // 用于接收Learner的连接,启动LearnerHandler
    LearnerCnxAcceptor cnxAcceptor;
    
    // list of all the followers
    // learnerHandler集合
    private final HashSet<LearnerHandler> learners = new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    // 参与者的LearnerHandler集合
    private final HashSet<LearnerHandler> forwardingFollowers = new HashSet<LearnerHandler>();

    //观察者LearnerHandler集合
    private final HashSet<LearnerHandler> observingLearners = new HashSet<LearnerHandler>();

    private final ProposalStats proposalStats;

    public ProposalStats getProposalStats() {
        return proposalStats;
    }

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    // 正在处理的同步(要等到过半ack才算完)
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs = new HashMap<Long,List<LearnerSyncRequest>>();
    
    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    // 只不过是一个临时变量，并不是Follower count，看调用方
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * Adds peer to the leader.
     * 
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     * 
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);            
        }        
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }        
    }
    
    ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new ProposalStats();
        try {
            if (self.getQuorumListenOnAllIPs()) {
                ss = new ServerSocket(self.getQuorumAddress().getPort());
            } else {
                ss = new ServerSocket();
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk=zk;
    }

    /**
     * This message is for follower to expect diff
     */
    // Leader->Learner，用于通知Learner服务器，Leader即将与其进行DIFF方式的数据同步
    // 代表LEADER后续会告诉LEARNER：不断接收PROPOSAL和COMMIT，一步步提交让LEARNER从B变成B1，到B2。。。最后到A的样子
    final static int DIFF = 13;
    
    /**
     * This is for follower to truncate its logs 
     */
    // 用于触发Learner服务器进行内存数据库的回滚操作
    // 回滚到某个zxid后，后面也会跟着一堆INFORM和COMMIT
    final static int TRUNC = 14;
    
    /**
     * This is for follower to download the snapshots
     */
    // 用于通知Learner服务器，Leader即将与其进行SNAP方式的数据同步
    // 代表LEADER直接告诉LEARNER:我长得样子是A，你copy一下变成我的样子就好了
    final static int SNAP = 15;

    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    // leader发给learner的，当leader发送了同步数据的相关的packets之后发送
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    //用于告知Learner服务器已经完成了数据同步，可以对外提供服务
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
        
    /**
     * This message type informs observers of a committed proposal.
     */
    // 通知observer 有commit消息
    final static int INFORM = 8;

    // 已经提出，还没有处理完的提议的map,key是zxid
    ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    // 即将生效的提议(已有过半确认)
    ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();

    //newLeader的提议
    Proposal newLeaderProposal = new Proposal();

    // 用于接收Learner的连接,启动LearnerHandler
    // LearnerCnxAcceptor负责接收所有非Leader服务器的连接请求。
    class LearnerCnxAcceptor extends ZooKeeperThread{

        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    try{
                        // 接收socket链接
                        Socket s = ss.accept();
                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(s.getInputStream());
                        // 除开Leader服务器，其他服务器都会与Leader建立连接，这个时候都会新建出一个LearnerHandler线程
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        // 启动LearnerHandler
                        fh.start();
                    } catch (SocketException e) {
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e){
                        LOG.error("Exception while connecting to quorum learner", e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e);
            }
        }
        
        public void halt() {
            stop = true;
        }
    }

    //leader状态总结
    StateSummary leaderStateSummary;

    //新的acceptEpoch号码,各端的max(lastAcceptedEpoch) + 1, 先默认-1
    long epoch = -1;

    // 是否在等待NewEpoch生成
    // 过半的机器都向Leader注册FOLLOWERINFO信息后，就认为Leader生成好了最新NewEpoch
    boolean waitingForNewEpoch = true;

    //是否准备好启动了
    volatile boolean readyToStart = false;
    
    /**
     * This method is main function that is called to lead
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    //选举完leader之后,Leader会调用该方法
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {}", electionTimeTaken);
        // 开始fast leader election时间
        self.start_fle = 0;
        // 结束时间
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            // 初始tick为0，代表第几个tickTime
            self.tick.set(0);
            // 先loadData加载数据
            zk.loadData();

            // 根据epoch和zxid记录状态
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // Start thread that waits for connection requests from 
            // new followers.
            //启动LearnerCnxAcceptor线程，等待learner的连接
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();
            
            readyToStart = true;

            // 阻塞等待生成最新Epoch
            // 如果learner的epoch比自己高，更新自己的
            // 通过过半验证后后唤醒Leader线程和其他的LearnerHandler线程
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());
            
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));
            
            synchronized(this){
                // 获取zxid
                lastProposed = zk.getZxid();
            }

            //生成NEWLEADER包
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),null, null);

            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            // 等待Epoch的过半机制验证
            waitForEpochAck(self.getId(), leaderStateSummary);

            //Epoch的过半机制验证后
            self.setCurrentEpoch(epoch);

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            try {
                // 等待NEWLEADER的过半机制验证
                waitForNewLeaderAck(self.getId(), zk.getZxid());
            } catch (InterruptedException e) {
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + getSidSetString(newLeaderProposal.ackSet) + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();
                for (LearnerHandler f : learners)
                    followerSet.add(f.getSid());
                    
                if (self.getQuorumVerifier().containsQuorum(followerSet)) {
                    LOG.warn("Enough followers present. "
                            + "Perhaps the initTicks need to be increased.");
                }
                Thread.sleep(self.tickTime);
                self.tick.incrementAndGet();
                return;
            }

            // 启动zk
            // Leader在自己发送NEWLEADER之后，过半参与者返回ACK，即可启动
            startZkServer();
            
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             * 
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }
            
            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.cnxnFactory.setZooKeeperServer(zk);
            }
            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration

            // 一个tick ping两次，
            boolean tickSkip = true;

            // 不断完成ping，保持过半参与者同步
            while (true) {
                // 每tickTime周期的一半
                Thread.sleep(self.tickTime / 2);
                if (!tickSkip) {
                    //每两次代表一个tick
                    self.tick.incrementAndGet();
                }
                HashSet<Long> syncedSet = new HashSet<Long>();

                // lock on the followers when we use it.
                syncedSet.add(self.getId());

                for (LearnerHandler f : getLearners()) {
                    // Synced set is used to check we have a supporting quorum, so only
                    // PARTICIPANT, not OBSERVER, learners should be used
                    // 如果是同步的，且是参与者
                    if (f.synced() && f.getLearnerType() == LearnerType.PARTICIPANT) {
                        syncedSet.add(f.getSid());
                    }
                    // 验证有没有LearnerHandler的proposal超时了没有处理
                    f.ping();
                }

                // check leader running status
                if (!this.isRunning()) {
                    shutdown("Unexpected internal error");
                    return;
                }
              // 需要验证时，集群验证过半验证失败
              if (!tickSkip && !self.getQuorumVerifier().containsQuorum(syncedSet)) {
                //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                    // Lost quorum, shutdown
                    shutdown("Not sufficient followers synced, only synced with sids: [ "
                            + getSidSetString(syncedSet) + " ]");
                    // make sure the order is the same!
                    // the leader goes to looking
                    return;
              }
              // 翻转，进入下半个tickTime
              tickSkip = !tickSkip;
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }
        
        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }
        
        // NIO should not accept conenctions
        self.cnxnFactory.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     * 
     * @param zxid
     *                the zxid of the proposal sent out
     * @param followerAddr
     */
    // 针对提议回复ACK的处理逻辑，过半验证了就通知所有Learner[]
    // 调用处：
    // 1）leader端：AckRequestProcessor调用了Leader#processAck
    // 2）Follower端：SendAckRequestProcessor 发出ACK后，被服务端的LearnerHandler处理到，最终也是调用Leader#processAck，
    //
    // 说白了就是收集ACK，如果通过过半验证了，就告诉所有参与者commit，告诉所有observer INFORM，然后调用commitProcessor逻辑
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        // log相关
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack by this method. However,
             * the learner sends ack back to the leader after it gets UPTODATE
             * so we just ignore the message.
             */
            return;
        }
        // 没有处理当中的提议
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        // 提议已经处理过了
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        // 获取zxid对应的提议
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }
        // 对应提议的ack集合添加sid记录
        p.ackSet.add(sid);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }
        // 过半回复ack
        if (self.getQuorumVerifier().containsQuorum(p.ackSet)){
            if (zxid != lastCommitted+1) {
                LOG.warn("Commiting zxid 0x{} from {} not first!",
                        Long.toHexString(zxid), followerAddr);
                LOG.warn("First is 0x{}", Long.toHexString(lastCommitted + 1));
            }
            // 该proposal已经处理完了
            outstandingProposals.remove(zxid);
            if (p.request != null) {
                // 即将应用的队列 添加该proposal
                toBeApplied.add(p);
            }
            if (p.request == null) {
                LOG.warn("Going to commmit null request for proposal: {}", p);
            }
            // 提交，发给所有参与者Follower
            commit(zxid);
            // 告诉所有观察者
            inform(p); // Observer同步
            // leader自己也提交
            zk.commitProcessor.commit(p.request);
            if(pendingSyncs.containsKey(zxid)){
                for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                    //发送同步请求给LearnerSyncRequest记录的server
                    sendSync(r);
                }
            }
        }
    }

    // 这是一个请求处理的调用链
    // 该处理器有一个toBeApplied队列，用来存储那些已经被CommitProcessor处理过的可被提交的Proposal。
    // 其会将这些请求交付给FinalRequestProcessor处理器处理，待其处理完后，再将其从toBeApplied队列中移除。
    static class ToBeAppliedRequestProcessor implements RequestProcessor {

        private RequestProcessor next;

        private ConcurrentLinkedQueue<Proposal> toBeApplied;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         * 
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next,
                ConcurrentLinkedQueue<Proposal> toBeApplied) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.toBeApplied = toBeApplied;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            // request.addRQRec(">tobe");
            next.processRequest(request);
            Proposal p = toBeApplied.peek();
            if (p != null && p.request != null
                    && p.request.zxid == request.zxid) {
                toBeApplied.remove();
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     * 
     * @param qp
     *                the packet to be sent
     */
    // QuorumPacket发给所有 参与者
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {                
                f.queuePacket(qp);
            }
        }
    }
    
    /**
     * send a packet to all observers     
     */
    //QuorumPacket发给所有 观察者
    void sendObserverPacket(QuorumPacket qp) {        
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    //最近commit的zxid
    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 
     * @param zxid
     */
    // 更新lastCommitted，生成commit包，发给所有参与者
    public void commit(long zxid) {
        // 更新最近commit的zxid
        synchronized(this){
            lastCommitted = zxid;
        }
        // 生成commit消息
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        // 发给所有参与者
        sendPacket(qp);
    }
    
    /**
     * Create an inform packet and send it to all observers.
     * @param proposal
     */
    //生成inform包,告诉所有observer
    public void inform(Proposal proposal) {
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid, proposal.packet.getData(), null);
        // 告诉所有observer
        sendObserverPacket(qp);
    }

    // 最近一次提议的zxid
    long lastProposed;

    
    /**
     * Returns the current epoch of the leader.
     * 
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }

    //Xid回滚异常，一般用于Xid低32位满了的时候用(高32位是epoch号码)
    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     * 
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    // 根据Request产生提议,发给所有参与者
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        // 如果zxid低的32位已经满了，为了不溢出，就关闭掉，重新选举
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            //xid回滚异常
            throw new XidRolloverException(msg);
        }
        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastProposalSize(data.length);

        // 产生提议
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);
        
        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }
            // 更新最近一次提议的zxid
            lastProposed = p.packet.getZxid();
            // 出现了一个尚未处理的提议
            outstandingProposals.put(lastProposed, p);
            // 提议发给所有参与者
            sendPacket(pp);
        }
        return p;
    }
            
    /**
     * Process sync requests
     * 
     * @param r the request
     */

    //处理同步请求????
    synchronized public void processSync(LearnerSyncRequest r){
        //没有正在处理的提议
        if(outstandingProposals.isEmpty()){
            // 发送同步请求给LearnerSyncRequest记录的server
            sendSync(r);
        } else {
            // 把当时最新的lastProposed记录下来
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            // lastProposed对应的记录添加进当前的request,这个列表的同步都是到lastProposed这个位置
            l.add(r);
            // 放入map，等到这个提议通过
            pendingSyncs.put(lastProposed, l);
        }
    }
        
    /**
     * Sends a sync message to the appropriate server
     * 
     * @param r
     */
    // 发送Sync请求给合适的server(LearnerSyncRequest记录的)
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }
                
    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     * 
     * @param handler handler of the follower
     * @return last proposed zxid
     */
    // 就是learner同步的时候，同步方式是和leader的commitLog相关的，在此期间，记录在内存中的提议，以及即将生效的提议也要告诉给learner
    // 让leader知道Follower在进行同步,另外看zk当前有没有新的提议,或者同步的信息发送过去的
    synchronized public long startForwarding(LearnerHandler handler, long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        // 如果自己的zxid比发给learner的zxid大
        if (lastProposed > lastSeenZxid) {
            // 处理toBeApplied
            for (Proposal p : toBeApplied) {
                // 即将生效的zxid，对应learner已经有记录了
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet.getZxid(), null, null);
                // 加入handler的发送队列
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            // 处理outstandingProposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    //添加packet
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            // 如果是参与者，就加到 参与者集合中
            addForwardingFollower(handler);
        } else {
            // 加入到观察者 集合中
            addObserverLearnerHandler(handler);
        }
                
        return lastProposed;
    }
    // VisibleForTesting
    // 连接上leader的sid集合
    protected Set<Long> connectingFollowers = new HashSet<Long>();

    // Epoch同步过程中使用， 核心就是：阻塞等待生成最新Epoch
    // 1） 被Leader和LearnerHandler调用，即获取集群最大的lastAcceptedEpoch+1，用于设置新的acceptedEpoch
    // 2） 这个方法会阻塞等待新的Epoch通过集群的过半验证, 通过过半验证后后唤醒Leader线程和其他的LearnerHandler线程
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized(connectingFollowers) {
            // 如果还在等待new epoch
            if (!waitingForNewEpoch) {
                return epoch;
            }
            // 选出leader以及learner中最大一个lastAcceptedEpoch
            if (lastAcceptedEpoch >= epoch) {
                // 更新自己的epoch为最大的+1,
                epoch = lastAcceptedEpoch+1;
            }
            // 连接的Learner添加记录
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            // 集群过半验证器
            QuorumVerifier verifier = self.getQuorumVerifier();
            // 如果自己也连接上,并且已经有过半机器连接上
            if (connectingFollowers.contains(self.getId()) && verifier.containsQuorum(connectingFollowers)) {
                // 不用再等了
                waitingForNewEpoch = false;
                // 设置acceptedEpoch
                self.setAcceptedEpoch(epoch);
                // 通知阻塞的线程，包括Leader 和 LearnerHandler线程
                connectingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                // 如果已经有机器连接上leader了，那么最多等待一段时间直到其他机器通过 过半验证
                while(waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                // 等待结束了还没有过半验证通过
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");        
                }
            }
            return epoch;
        }
    }
    // 针对Leader的 LEADERINFO 回复 ACKEPOCH的集合
    protected Set<Long> electingFollowers = new HashSet<Long>();
    // 过半的机器 针对Leader的 LEADERINFO回复 ACKEPOCH的消息
    protected boolean electionFinished = false;

    //  Epoch同步过程中使用
    //  Leader生成最新的Epoch后，会向Learner发送LEADERINFO消息
    //  这个方法就是等待过半机器(Learner和leader)针对LEADERINFO回复 ACKEPOCH
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized(electingFollowers) {
            // 如果已经过半返回了
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                // 如果有currentEpoch，zxid比Leader自己的还要新
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                if (isParticipant(id)) {
                    electingFollowers.add(id);
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            //验证通过则唤醒所有的LearnerHandler线程
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {                
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                // 不验证通过则阻塞等待一段时间
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                // 等待结束了还没有过半验证通过
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string  
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     */
    // 启动zk
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + getSidSetString(newLeaderProposal.ackSet)
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        // 启动服务器
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     *
     * @param sid
     * @throws InterruptedException
     *
     * LearnerHandler是一个线程，所有所有的LearnerHandler会公用一个leader
     */
    // 等到有过半的参与者针对Leader发出的NEWLEADER返回ACK
    // 针对Leader的NEWLEADER消息回复了Ack【数据同步过程中使用】
    public void waitForNewLeaderAck(long sid, long zxid) throws InterruptedException {

        synchronized (newLeaderProposal.ackSet) {

            // 如果已经过半同步了
            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();

            // 接受到的的zxid不对
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            // 参与者返回的ACK才算数
            if (isParticipant(sid)) {
                newLeaderProposal.ackSet.add(sid);
            }

            // 判断新leader这次投票的ack集合是否可以集群验证通过（过半）
            if (self.getQuorumVerifier().containsQuorum(newLeaderProposal.ackSet)) {
                // 标记已经过半同步了
                quorumFormed = true;
                newLeaderProposal.ackSet.notifyAll();
            } else {
                // 如果暂时没有验证过，就先wait,等待其他learner线程给ack过来
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                // 先同步的，等待一段时间，等过半的机器都同步
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.ackSet.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                // 等了一段时间还没有过半同步，就报错
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        default:
            return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getVotingView().containsKey(sid);
    }
}
