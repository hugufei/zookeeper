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

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ObserverBean;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Observers are peers that do not take part in the atomic broadcast protocol.
 * Instead, they are informed of successful proposals by the Leader. Observers
 * therefore naturally act as a relay point for publishing the proposal stream
 * and can relieve Followers of some of the connection load. Observers may
 * submit proposals, but do not vote in their acceptance. 
 *
 * See ZOOKEEPER-368 for a discussion of this feature. 
 */
// Observer充当观察者角色，观察Zookeeper集群的最新状态变化并将这些状态同步过来，
// 其对于非事务请求可以进行独立处理，对于事务请求，则会转发给Leader服务器进行处理。
// Observer不会参与任何形式的投票，包括事务请求Proposal的投票和Leader选举投票。
public class Observer extends Learner{      

    Observer(QuorumPeer self,ObserverZooKeeperServer observerZooKeeperServer) {
        this.self = self;
        this.zk=observerZooKeeperServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Observer ").append(sock);        
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }
    
    /**
     * the main method called by the observer to observe the leader
     *
     * @throws InterruptedException
     */
    void observeLeader() throws InterruptedException {
        zk.registerJMX(new ObserverBean(this, zk), self.jmxLocalPeerBean);

        try {
            QuorumServer leaderServer = findLeader();
            LOG.info("Observing " + leaderServer.addr);
            try {
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                long newLeaderZxid = registerWithLeader(Leader.OBSERVERINFO);

                syncWithLeader(newLeaderZxid);
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    readPacket(qp);
                    processPacket(qp);                   
                }
            } catch (Exception e) {
                LOG.warn("Exception when observing the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
    
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }
    
    /**
     * Controls the response of an observer to the receipt of a quorumpacket
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws IOException{
        switch (qp.getType()) {
        case Leader.PING:
            ping(qp);
            break;
        case Leader.PROPOSAL:
            LOG.warn("Ignoring proposal");
            break;
        case Leader.COMMIT:
            LOG.warn("Ignoring commit");            
            break;            
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Observer started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            ((ObserverZooKeeperServer)zk).sync();
            break;
        case Leader.INFORM:            
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
            Request request = new Request (null, hdr.getClientId(), 
                                           hdr.getCxid(),
                                           hdr.getType(), null, null);
            request.txn = txn;
            request.hdr = hdr;
            ObserverZooKeeperServer obs = (ObserverZooKeeperServer)zk;
            obs.commitRequest(request);            
            break;
        default:
            LOG.error("Invalid packet type: {} received by Observer", qp.getType());
        }
    }

    /**
     * Shutdown the Observer.
     */
    public void shutdown() {       
        LOG.info("shutdown called", new Exception("shutdown Observer"));
        super.shutdown();
    }
}

