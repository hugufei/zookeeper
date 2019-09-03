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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 */

//ClientCnxn是Zookeeper客户端中负责维护客户端与服务端之间的网络连接并进行一系列网络通信的核心工作类
public class ClientCnxn {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    private static final String ZK_SASL_CLIENT_USERNAME =
        "zookeeper.sasl.client.username";

    /* ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;

    /** This controls whether automatic watch resetting is enabled.
     * Clients automatically reset watches during session reconnect, this
     * option allows the client to turn off this behavior by setting
     * the environment variable "zookeeper.disableAutoWatchReset" to "true" */
    private static boolean disableAutoWatchReset;
    static {
        // this var should not be public, but otw there is no easy way
        // to test
        disableAutoWatchReset =
            Boolean.getBoolean("zookeeper.disableAutoWatchReset");
        if (LOG.isDebugEnabled()) {
            LOG.debug("zookeeper.disableAutoWatchReset is "
                    + disableAutoWatchReset);
        }
    }

    static class AuthData {
        AuthData(String scheme, byte data[]) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte data[];
    }

    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     */
    // 是已经从client发送，但是要等待server响应的packet队列
    // 但是：auth和ping以及正在处理的sasl没有加入pendingQueue,触发的watch也没有在pendingQueue中.
    private final LinkedList<Packet> pendingQueue = new LinkedList<Packet>();

    /**
     * These are the packets that need to be sent.
     */
    // 是请求发送队列，是client存储需要被发送到server端的Packet队列
    private final LinkedList<Packet> outgoingQueue = new LinkedList<Packet>();

    private int connectTimeout;

    /**
     * The timeout in ms the client negotiated with the server. This is the
     * "real" timeout, not the timeout request by the client (which may have
     * been increased/decreased by the server which applies bounds to this
     * value.
     */
    private volatile int negotiatedSessionTimeout;

    private int readTimeout;

    private final int sessionTimeout;

    private final ZooKeeper zooKeeper;

    private final ClientWatchManager watcher;

    private long sessionId;

    private byte sessionPasswd[] = new byte[16];

    /**
     * If true, the connection is allowed to go to r-o mode. This field's value
     * is sent, besides other data, during session creation handshake. If the
     * server on the other side of the wire is partitioned it'll accept
     * read-only clients only.
     */
    private boolean readOnly;

    final String chrootPath;

    final SendThread sendThread;

    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    private volatile boolean closing = false;
    
    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    private final HostProvider hostProvider;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     */
    volatile boolean seenRwServerBefore = false;


    public ZooKeeperSaslClient zooKeeperSaslClient;

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb
            .append("sessionid:0x").append(Long.toHexString(getSessionId()))
            .append(" local:").append(local)
            .append(" remoteserver:").append(remote)
            .append(" lastZxid:").append(lastZxid)
            .append(" xid:").append(xid)
            .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
            .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
            .append(" queuedpkts:").append(outgoingQueue.size())
            .append(" pendingresp:").append(pendingQueue.size())
            .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     */
    // Packet是ClientCnxn内部定义的一个堆协议层的封装，用作Zookeeper中请求和响应的载体。
    // Packet包含了请求头（requestHeader）、响应头（replyHeader）、请求体（request）、响应体（response）、节点路径（clientPath/serverPath）、注册的Watcher（watchRegistration）等信息
    // 然而，并非Packet中所有的属性都在客户端与服务端之间进行网络传输，只会将requestHeader、request、readOnly三个属性序列化，并生成可用于底层网络传输的ByteBuffer，
    // 其他属性都保存在客户端的上下文中，不会进行与服务端之间的网络传输。
    static class Packet {
        // 请求头
        RequestHeader requestHeader;
        // 响应头
        ReplyHeader replyHeader;
        // 请求体
        Record request;
        // 响应体
        Record response;

        //序列化之后的byteBuffer
        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        //client节点路径，不含chrootPath
        String clientPath; //客户端节点路径
        /** Servers's view of the path (may differ due to chroot) **/
        //server节点路径,含chrootPath
        String serverPath;

        // 是否结束(已经得到响应才能结束)
        boolean finished;

        //异步回调
        AsyncCallback cb;

        //上下文
        Object ctx;

        // 注册的Watcher
        WatchRegistration watchRegistration;

        // 只读
        public boolean readOnly;

        /** Convenience ctor */
        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response,
                 watchRegistration, false);
        }

        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration, boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        //序列化创建byteBuffer记录在bb字段中
        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                // 序列化请求头，包含xid和type
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    //序列化request(对于特定请求如GetDataRequest,包含了是否存在watcher的标志位)
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                LOG.warn("Ignoring unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly)
            throws IOException {
        this(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher,
             clientCnxnSocket, 0, new byte[16], canBeReadOnly);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly) {
        this.zooKeeper = zooKeeper;
        this.watcher = watcher;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.sessionTimeout = sessionTimeout;
        this.hostProvider = hostProvider;
        this.chrootPath = chrootPath;

        connectTimeout = sessionTimeout / hostProvider.size();
        readTimeout = sessionTimeout * 2 / 3;
        readOnly = canBeReadOnly;

        sendThread = new SendThread(clientCnxnSocket);
        eventThread = new EventThread();

    }

    /**
     * tests use this to check on reset of watches
     * @return if the auto reset of watches are disabled
     */
    public static boolean getDisableAutoResetWatch() {
        return disableAutoWatchReset;
    }
    /**
     * tests use this to set the auto reset
     * @param b the value to set disable watches to
     */
    public static void setDisableAutoResetWatch(boolean b) {
        disableAutoWatchReset = b;
    }
    public void start() {
        sendThread.start();
        eventThread.start();
    }

    private Object eventOfDeath = new Object();

    //将Event以及对应需要触发的watches集合进行组合绑定
    private static class WatcherSetEventPair {
        // 事件触发需要被通知的watches集合
        private final Set<Watcher> watchers;
        // 事件
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }
    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().
            replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     * EventThread是客户端ClientCnxn内部的一个事件处理线程，负责客户端的事件处理，并触发客户端注册的Watcher监听。
     * 1) 从waitingEvents队列中获取数据
     * 2) 如果是watcher事件通知，触发绑定的watcher逻辑
     * 3) 如果是异步请求，则调用对应的异步回调函数
     */
    class EventThread extends ZooKeeperThread {
        // 临时存放需要被触发的Obj，包含Watcher和AsyncCallBack
        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        // 当前session状态
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        // 如果需要停止(并没有真的kill)
        private volatile boolean wasKilled = false;

        // 线程是否在运行(waitingEvents是否在工作)
        private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        // 将WatchedEvent加入waitingEvents队列
        public void queueEvent(WatchedEvent event) {
            if (event.getType() == EventType.None && sessionState == event.getState()) {
                return;
            }
            sessionState = event.getState();

            // materialize the watchers based on the event
            // 用WatcherSetEventPair封装watchers和watchedEvent
            // 这里用到了client中watch的记录,进行相应的get和remove操作
            WatcherSetEventPair pair = new WatcherSetEventPair(
                    watcher.materialize(event.getState(), event.getType(), event.getPath()), event);
            // queue the pair (watch set & event) for later processing
            // 把通知事件加到waitingEvents
            waitingEvents.add(pair);
        }

        // 当Packet有callback的时候调用。这个Packet带了AsyncCallback
        public void queuePacket(Packet packet) {
            // 如果kill掉了
            if (wasKilled) {
             synchronized (waitingEvents) {
                if (isRunning)
                    //线程在运行，则加入临时队列
                    waitingEvents.add(packet);
                else
                    processEvent(packet);//同步处理Event
             }
          } else {
             waitingEvents.add(packet); // 加入临时队列
          }
       }

       // 将eventOfDeath加入waitingEvents代表线程将要kill(进行take时才标示wasKilled)
        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        // 会不断地从watingEvents中取出Object，识别具体类型（Watcher或AsyncCallback），并分别调用process和processResult接口方法来实现对事件的触发和回调。
        // 根据eventOfDeath以及waitingEvents.isEmpty()改变两个标示位wasKilled,isRunning
        @Override
        public void run() {
            try {
              isRunning = true;
              while (true) {
                 Object event = waitingEvents.take();
                 if (event == eventOfDeath) {
                    wasKilled = true;
                 } else {
                     // 核心方法
                     processEvent(event);
                 }
                 if (wasKilled)
                    synchronized (waitingEvents) {
                       //当需要被停掉，且临时队列处理完了
                       if (waitingEvents.isEmpty()) {
                          isRunning = false;
                          break;//跳出while，结束线程
                       }
                    }
              }
           } catch (InterruptedException e) {
              LOG.error("Event thread exiting due to interruption", e);
           }

            LOG.info("EventThread shut down for session: 0x{}", Long.toHexString(getSessionId()));
        }

        //对WatchedEvent和Packet两种事件进行处理
       private void processEvent(Object event) {
          try {
              // 如果是watch类型
              if (event instanceof WatcherSetEventPair) {
                  // each watcher will process the event
                  WatcherSetEventPair pair = (WatcherSetEventPair) event;
                  // 从WatcherSetEventPair这个wraper中取出watchers和event
                  for (Watcher watcher : pair.watchers) {
                      try {
                          // 每个watch一次处理event，调用具体的实现类
                          watcher.process(pair.event);
                      } catch (Throwable t) {
                          LOG.error("Error while calling watcher ", t);
                      }
                  }
              } else {// 否则就是AsyncCallBack类型事件
                  Packet p = (Packet) event;
                  int rc = 0;
                  String clientPath = p.clientPath;
                  if (p.replyHeader.getErr() != 0) {
                      rc = p.replyHeader.getErr();
                  }
                  if (p.cb == null) {
                      LOG.warn("Somehow a null cb got to EventThread!");
                  } else if (p.response instanceof ExistsResponse
                          || p.response instanceof SetDataResponse
                          || p.response instanceof SetACLResponse) {
                      StatCallback cb = (StatCallback) p.cb;
                      if (rc == 0) {
                          if (p.response instanceof ExistsResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((ExistsResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetDataResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetDataResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetACLResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetACLResponse) p.response)
                                              .getStat());
                          }
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetDataResponse) {
                      DataCallback cb = (DataCallback) p.cb;
                      GetDataResponse rsp = (GetDataResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getData(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetACLResponse) {
                      ACLCallback cb = (ACLCallback) p.cb;
                      GetACLResponse rsp = (GetACLResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getAcl(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetChildrenResponse) {
                      ChildrenCallback cb = (ChildrenCallback) p.cb;
                      GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetChildren2Response) {
                      Children2Callback cb = (Children2Callback) p.cb;
                      GetChildren2Response rsp = (GetChildren2Response) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null, null);
                      }
                  } else if (p.response instanceof CreateResponse) {
                      StringCallback cb = (StringCallback) p.cb;
                      CreateResponse rsp = (CreateResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx,
                                  (chrootPath == null
                                          ? rsp.getPath()
                                          : rsp.getPath()
                                    .substring(chrootPath.length())));
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof MultiResponse) {
                          MultiCallback cb = (MultiCallback) p.cb;
                          MultiResponse rsp = (MultiResponse) p.response;
                          if (rc == 0) {
                                  List<OpResult> results = rsp.getResultList();
                                  int newRc = rc;
                                  for (OpResult result : results) {
                                          if (result instanceof ErrorResult
                                              && KeeperException.Code.OK.intValue()
                                                  != (newRc = ((ErrorResult) result).getErr())) {
                                                  break;
                                          }
                                  }
                                  cb.processResult(newRc, clientPath, p.ctx, results);
                          } else {
                                  cb.processResult(rc, clientPath, p.ctx, null);
                          }
                  }  else if (p.cb instanceof VoidCallback) {
                      VoidCallback cb = (VoidCallback) p.cb;
                      cb.processResult(rc, clientPath, p.ctx);
                  }
              }
          } catch (Throwable t) {
              LOG.error("Caught unexpected throwable", t);
          }
       }
    }

    // 如果Packet中有通知，则调用watchRegistration的注册方法
    private void finishPacket(Packet p) {
        // 如果有要注册的watchRegistration
        if (p.watchRegistration != null) {
            // 根据response code进行注册
            p.watchRegistration.register(p.replyHeader.getErr());
        }

        if (p.cb == null) {
            // 操作同步通知，唤醒在p上等待的线程
            synchronized (p) {
                p.finished = true;
                p.notifyAll();
            }
        } else {
            // 操作异步通知
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch (state) {
        case AUTH_FAILED:
            p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
            break;
        case CLOSED:
            p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
            break;
        default:
            p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    private volatile long lastZxid;

    public long getLastZxid() {
        return lastZxid;
    }

    static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }
        
        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    private static class SessionTimeoutException extends IOException {
        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }
    }
    
    private static class SessionExpiredException extends IOException {
        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }
    }

    private static class RWServerFoundException extends IOException {
        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }
    }
    
    public static final int packetLen = Integer.getInteger("jute.maxbuffer",
            4096 * 1024);

    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     *
     * SendThread是客户端ClientCnxn内部的一个核心I/O调度线程，用于管理客户端与服务端之间的所有网络I/O操作.
     * 在Zookeeper客户端实际运行中，SendThread的作用如下：
     * 1) 维护了客户端与服务端之间的会话生命周期（通过一定周期频率内向服务端发送PING包检测心跳），如果会话周期内客户端与服务端出现TCP连接断开，那么就会自动且透明地完成重连操作。
     * 2) 管理了客户端所有的请求发送和响应接收操作，其将上层客户端API操作转换成相应的请求协议并发送到服务端，并完成对同步调用的返回和异步调用的回调。
     * 3) 将来自服务端的事件传递给EventThread去处理。
     */
    // 大体连接过程:
    // 首先与ZooKeeper服务器建立连接，有两层连接要建立。
    // 1) 客户端与服务器端的TCP连接
    // 2) 在TCP连接的基础上建立session关联
    //
    // 建立TCP连接之后，客户端发送ConnectRequest请求，申请建立session关联，此时服务器端会为该客户端分配sessionId和密码，同时开启对该session是否超时的检测。
    // 当在sessionTimeout时间内，即还未超时，此时TCP连接断开，服务器端仍然认为该sessionId处于存活状态。
    // 此时，客户端会选择下一个ZooKeeper服务器地址进行TCP连接建立，TCP连接建立完成后，拿着之前的sessionId和密码发送ConnectRequest请求，如果还未到该sessionId的超时时间，则表示自动重连成功。
    // 对客户端用户是透明的，一切都在背后默默执行，ZooKeeper对象是有效的。
    //
    // 如果重新建立TCP连接后，已经达到该sessionId的超时时间了（服务器端就会清理与该sessionId相关的数据），则返回给客户端的sessionTimeout时间为0，sessionid为0，密码为空字节数组。
    // 客户端接收到该数据后，会判断协商后的sessionTimeout时间是否小于等于0，如果小于等于0，则使用eventThread线程先发出一个KeeperState.Expired事件，通知相应的Watcher。
    // 然后结束EventThread线程的循环，开始走向结束。此时ZooKeeper对象就是无效的了，必须要重新new一个新的ZooKeeper对象，分配新的sessionId了。
    class SendThread extends ZooKeeperThread {

        // 上一次ping的 nano time
        private long lastPingSentNs;

        // 通信层ClientCnxnSocket
        private final ClientCnxnSocket clientCnxnSocket;

        // 随机数生成器
        private Random r = new Random(System.nanoTime());

        // 是否第一次connect
        private boolean isFirstConnect = true;

        // 读取server的回复，进行outgoingQueue以及pendingQueue的相关处理，事件触发等等
        // 1.处理ping命令,AuthPacket,WatcherEvent,验证sasl并返回
        // 2.从pendingQueue取出packet进行验证(有顺序保证)
        // 3.调用finishPacket完成AsyncCallBack处理以及watcher的注册

        // PS: auth和ping以及正在处理的sasl没有加入pendingQueue,
        // 触发的watch也没有在pendingQueue中(是server主动发过来的),
        // 而AsyncCallBack在pendingQueue中(见finishPacket)
        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            //反序列化出 回复头
            replyHdr.deserialize(bbia, "header");
            if (replyHdr.getXid() == -2) { // ping 的结果
                // -2 is the xid for pings
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got ping response for sessionid: 0x"
                            + Long.toHexString(sessionId)
                            + " after "
                            + ((System.nanoTime() - lastPingSentNs) / 1000000)
                            + "ms");
                }
                return;
            }
            if (replyHdr.getXid() == -4) { // 认证结果
                // -4 is the xid for AuthPacket               
                if(replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    state = States.AUTH_FAILED;                    
                    eventThread.queueEvent( new WatchedEvent(Watcher.Event.EventType.None, 
                            Watcher.Event.KeeperState.AuthFailed, null) );            		            		
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got auth sessionid:0x"
                            + Long.toHexString(sessionId));
                }
                return;
            }

            //-1代表通知类型 即WatcherEvent
            if (replyHdr.getXid() == -1) { // watch事件通知
                // -1 means notification
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got notification sessionid:0x"
                        + Long.toHexString(sessionId));
                }

                //反序列化WatcherEvent
                WatcherEvent event = new WatcherEvent();
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                // 把serverPath转化成clientPath
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if(serverPath.compareTo(chrootPath)==0)
                        event.setPath("/");
                    else if (serverPath.length() > chrootPath.length())
                        event.setPath(serverPath.substring(chrootPath.length()));
                    else {
                    	LOG.warn("Got server path " + event.getPath()
                    			+ " which is too short for chroot path "
                    			+ chrootPath);
                    }
                }
                // WatcherEvent还原成WatchedEvent
                WatchedEvent we = new WatchedEvent(event);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got " + we + " for sessionid 0x"
                            + Long.toHexString(sessionId));
                }

                // 把通知事件加到waitingEvents,交给eventThread处理
                eventThread.queueEvent( we );
                return;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (clientTunneledAuthenticationInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia,"token");
                zooKeeperSaslClient.respondToServer(request.getToken(),
                  ClientCnxn.this);
                return;
            }

            // 处理响应，从pendingQueue中取出数据
            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + replyHdr.getXid());
                }
                //得到了response，从pendingQueue中移除
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             */
            try {
                // 如果请求和响应的顺序不一样，会报错
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(
                            KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid "
                            + replyHdr.getXid() + " with err " +
                            + replyHdr.getErr() +
                            " expected Xid "
                            + packet.requestHeader.getXid()
                            + " for a packet with details: "
                            + packet );
                }

                packet.replyHeader.setXid(replyHdr.getXid());
                // 把服务的报错信息设置进来
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    packet.response.deserialize(bbia, "response");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading reply sessionid:0x"
                            + Long.toHexString(sessionId) + ", packet:: " + packet);
                }
            } finally {
                // 处理packet
                finishPacket(packet);
            }
        }

        SendThread(ClientCnxnSocket clientCnxnSocket) {
            super(makeThreadName("-SendThread()"));
            state = States.CONNECTING;
            this.clientCnxnSocket = clientCnxnSocket;
            // 绑定为后台线程
            setDaemon(true);
        }

        // TODO: can not name this method getState since Thread.getState()
        // already exists
        // It would be cleaner to make class SendThread an implementation of
        // Runnable
        /**
         * Used by ClientCnxnSocket
         *
         * 获取zk的状态
         * @return
         */
        ZooKeeper.States getZkState() {
            return state;
        }

        // 获取通信层clientCnxnSocket
        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        // 主要连接函数，用于将watches和authData传给server，允许clientCnxnSocket可读写
        // 1) 根据之前是否连接过设置sessId以及生成ConnectRequest
        // 2) 根据disableAutoWatchReset将已有的watches和authData以及放入outgoingQueue准备发送
        // 3) 允许clientCnxnSocket可读写,表示和server准备IO

        //需要注意：
        // 1) sessId :如果之前连接过，那么重连用之前的sessionId，否则默认0,重连参见ClientCnxn.SendThread#startConnect的调用
        // 2) 什么时候连接会有watches需要去注册?重连且disableAutoWatchReset为false的时候
        // 3) ConnectRequest是放在outgoingQueue第一个的，确保最先发出去的是连接请求(保证了第一个response是被ClientCnxnSocket#readConnectResult处理)
        void primeConnection() throws IOException {
            LOG.info("Socket connection established to "
                     + clientCnxnSocket.getRemoteSocketAddress()
                     + ", initiating session");
            isFirstConnect = false;
            // 如果之前连接过，那么重连用之前的sessionId，否则默认0,重连参见ClientCnxn.SendThread#startConnect的调用
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            // 生成ConnectRequest
            ConnectRequest conReq = new ConnectRequest(0, lastZxid,
                    sessionTimeout, sessId, sessionPasswd);
            synchronized (outgoingQueue) {
                // We add backwards since we are pushing into the front
                // Only send if there's a pending watch
                // TODO: here we have the only remaining use of zooKeeper in
                // this class. It's to be eliminated!
                // 建立连接的时候，需要设置监听器
                if (!disableAutoWatchReset) {
                    List<String> dataWatches = zooKeeper.getDataWatches();
                    List<String> existWatches = zooKeeper.getExistWatches();
                    List<String> childWatches = zooKeeper.getChildWatches();
                    if (!dataWatches.isEmpty()
                                || !existWatches.isEmpty() || !childWatches.isEmpty()) {

                        Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                        Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                        Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                        long setWatchesLastZxid = lastZxid;

                        while (dataWatchesIter.hasNext()
                                       || existWatchesIter.hasNext() || childWatchesIter.hasNext()) {
                            List<String> dataWatchesBatch = new ArrayList<String>();
                            List<String> existWatchesBatch = new ArrayList<String>();
                            List<String> childWatchesBatch = new ArrayList<String>();
                            int batchLength = 0;

                            // Note, we may exceed our max length by a bit when we add the last
                            // watch in the batch. This isn't ideal, but it makes the code simpler.
                            while (batchLength < SET_WATCHES_MAX_LENGTH) {
                                final String watch;
                                if (dataWatchesIter.hasNext()) {
                                    watch = dataWatchesIter.next();
                                    dataWatchesBatch.add(watch);
                                } else if (existWatchesIter.hasNext()) {
                                    watch = existWatchesIter.next();
                                    existWatchesBatch.add(watch);
                                } else if (childWatchesIter.hasNext()) {
                                    watch = childWatchesIter.next();
                                    childWatchesBatch.add(watch);
                                } else {
                                    break;
                                }
                                batchLength += watch.length();
                            }

                            // 设置watches
                            SetWatches sw = new SetWatches(setWatchesLastZxid,
                                    dataWatchesBatch,
                                    existWatchesBatch,
                                    childWatchesBatch);
                            RequestHeader h = new RequestHeader();
                            h.setType(ZooDefs.OpCode.setWatches);
                            h.setXid(-8);
                            Packet packet = new Packet(h, new ReplyHeader(), sw, null, null);
                            // 加入发送队列
                            outgoingQueue.addFirst(packet);
                        }
                    }
                }
                // authInfo加入发送队列
                for (AuthData id : authInfo) {
                    outgoingQueue.addFirst(new Packet(new RequestHeader(-4,
                            OpCode.auth), null, new AuthPacket(0, id.scheme,
                            id.data), null, null));
                }
                // ConnectRequest确保在发送队列的第一个
                outgoingQueue.addFirst(new Packet(null, null, conReq,
                            null, null, readOnly));
            }
            // 开启读写，这样outgoingQueue内容就可以发出去了
            clientCnxnSocket.enableReadWriteOnly();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session establishment request sent on "
                        + clientCnxnSocket.getRemoteSocketAddress());
            }
        }

        //根据clientPath以及chrootPath得到serverPath
        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        // ping命令，记录发出时间，生成请求，加入outgoingQueue待发送
        // 不论client处于什么模式，都要进行的心跳验证
        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            RequestHeader h = new RequestHeader(-2, OpCode.ping);
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        //读写server地址
        private InetSocketAddress rwServerAddress = null;

        //最短ping 读写server的 timeout时间
        private final static int minPingRwTimeout = 100;

        //最长ping 读写server的 timeout时间
        private final static int maxPingRwTimeout = 60000;

        //默认ping 读写server的 timeout时间
        private int pingRwTimeout = minPingRwTimeout;

        // Set to true if and only if constructor of ZooKeeperSaslClient
        // throws a LoginException: see startConnect() below.
        // sasl登录失败
        private boolean saslLoginFailed = false;

        // 开始连接，主要是和server的socket完成connect和accept
        // ps: startConnect和primeConnection区别是什么??
        // 两者的区别在于NIO的SelectionKey,前者限于connect和accept
        // 后者已经连接完成，开始了write和read，准备开始和zk server完成socket io
        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;
            state = States.CONNECTING;

            setName(getName().replaceAll("\\(.*\\)",
                    "(" + addr.getHostName() + ":" + addr.getPort() + ")"));
            if (ZooKeeperSaslClient.isEnabled()) {
                try {
                    String principalUserName = System.getProperty(
                            ZK_SASL_CLIENT_USERNAME, "zookeeper");
                    zooKeeperSaslClient =
                        new ZooKeeperSaslClient(
                                principalUserName+"/"+addr.getHostName());
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn("SASL configuration failed: " + e + " Will continue connection to Zookeeper server without "
                      + "SASL authentication, if Zookeeper server allows it.");
                    eventThread.queueEvent(new WatchedEvent(
                      Watcher.Event.EventType.None,
                      Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            logStartConnect(addr);
            //开始连接
            clientCnxnSocket.connect(addr);
        }

        //记录开始连接的日志
        private void logStartConnect(InetSocketAddress addr) {
            String msg = "Opening socket connection to server " + addr;
            if (zooKeeperSaslClient != null) {
              msg += ". " + zooKeeperSaslClient.getConfigStatus();
            }
            LOG.info(msg);
        }

        private static final String RETRY_CONN_MSG =
            ", closing socket connection and attempting reconnect";

        // 线程方法，完成了连接验证，超时检测，ping命令，以及网络IO
        // 注意： 在run方法会将outgoingQueue的内容发送出去，在ClientCnxnSocketNIO#doIO中，ping命令的packet是没有进入pendingQueue的
        // 1) clientCnxnSocket相关初始化
        // 2) 不断检测clientCnxnSocket是否和服务器处于连接状态,没有连接则进行连接
        // 3) 检测是否超时：当处于连接状态时，检测是否读超时，当处于未连接状态时，检测是否连接超时
        // 4) 不断的发送ping通知，服务器端每接收到ping请求，就会从当前时间重新计算session过期时间，所以当客户端按照一定时间间隔不断的发送ping请求，就能保证客户端的session不会过期
        // 5) 如果当前是只读的话，不断去找有没有支持读写的server
        // 6) 不断进行IO操作，发送请求队列中的请求和读取服务器端的响应数据
        // 7) !state.isAlive()时，进行相关清理工作

        @Override
        public void run() {
            //1） clientCnxnSocket初始化
            clientCnxnSocket.introduce(this,sessionId);
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
            int to;
            long lastPingRwServer = Time.currentElapsedTime();
            final int MAX_SEND_PING_INTERVAL = 10000; //10 seconds
            InetSocketAddress serverAddress = null;
            while (state.isAlive()) {
                try {
                    // 2） 检测clientCnxnSocket是否和服务器处于连接状态,没有连接则进行连接
                    if (!clientCnxnSocket.isConnected()) { //如果clientCnxnSocket的SelectionKey为null
                        // 不是第一次连接，就随机等待...，重试，随机出时间进行重试
                        if(!isFirstConnect){
                            try {
                                Thread.sleep(r.nextInt(1000));
                            } catch (InterruptedException e) {
                                LOG.warn("Unexpected exception", e);
                            }
                        }
                        // don't re-establish connection if we are closing
                        if (closing || !state.isAlive()) {
                            break;
                        }

                        // 读写服务端地址，暂时没有就从hostProvider中取一个
                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            // 取下一个地址
                            serverAddress = hostProvider.next(1000);
                        }
                        // 建立连接，socket建立成功后会调用SendThread.primeConnection()
                        startConnect(serverAddress);
                        // 更新时间
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    // 3） 检测是否超时,分为读超时和连接超时
                    if (state.isConnected()) {
                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                   LOG.error("SASL authentication with Zookeeper Quorum member failed: " + e);
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent == true) {
                                eventThread.queueEvent(new WatchedEvent(
                                      Watcher.Event.EventType.None,
                                      authState,null));
                            }
                        }
                        // 如果已经连接上，预计读超时时间 - 距离上次读已经过去的时间
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {
                        // //如果没连接上,预计连接超时时间 - 上次读已经过去的时间
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    // 如果连接失败，看是否超过了connectTimeout，超过了则抛异常SessionTimeoutException，但这个异常会被上层catch住，尝试重连
                    if (to <= 0) {
                        String warnInfo;
                        warnInfo = "Client session timed out, have not heard from server in "
                            + clientCnxnSocket.getIdleRecv()
                            + "ms"
                            + " for sessionid 0x"
                            + Long.toHexString(sessionId);
                        LOG.warn(warnInfo);
                        throw new SessionTimeoutException(warnInfo);
                    }

                    //4) 不断的发送ping通知
                    if (state.isConnected()) {
                        // 如果连接成功，就不断的ping
                        //1000(1 second) is to prevent race condition missing to send the second ping
                    	//also make sure not to send too many pings when readTimeout is small 
                        int timeToNextPing = readTimeout / 2 - clientCnxnSocket.getIdleSend() - 
                        		((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        //send a ping request either time is due or no packet sent out within MAX_SEND_PING_INTERVAL
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            // 进行ping,把ping包也加入到outgoingQueue中
                            sendPing();
                            clientCnxnSocket.updateLastSend();
                        } else {
                            // 如果预计下次ping的时间 < 实际距离下次ping的时间
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    // If we are in read-only mode, seek for read/write server
                    // 5) client连接了只读的server时，不断根据hostProvider找到一个可读写的server
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout = Math.min(2*pingRwTimeout, maxPingRwTimeout);
                            // ping读写server【如果ping到的是读写服务器，就抛异常，然后进行重试,看下面的cleanup()方法】
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }
                    // 6） 不断进行IO操作，发送请求队列中的请求和读取服务器端的响应数据, 传输的是outgoingQueue队列中的Packet
                    clientCnxnSocket.doTransport(to, pendingQueue, outgoingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    if (closing) {
                        if (LOG.isDebugEnabled()) {
                            // closing so this is expected
                            LOG.debug("An exception was thrown while closing send thread for session 0x"
                                    + Long.toHexString(getSessionId())
                                    + " : " + e.getMessage());
                        }
                        break;
                    } else {
                        // this is ugly, you have a better way speak up
                        if (e instanceof SessionExpiredException) {
                            LOG.info(e.getMessage() + ", closing socket connection");
                        } else if (e instanceof SessionTimeoutException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof EndOfStreamException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof RWServerFoundException) {
                            LOG.info(e.getMessage());
                        } else if (e instanceof SocketException) {
                            LOG.info("Socket error occurred: {}: {}", serverAddress, e.getMessage());
                        } else {
                            LOG.warn("Session 0x{} for server {}, unexpected error{}",
                                            Long.toHexString(getSessionId()),
                                            serverAddress,
                                            RETRY_CONN_MSG,
                                            e);
                        }
                        // cleanUp调用后使得ClientCnxnSocketNIO#isConnected为false，
                        // 因此ClientCnxn.SendThread#run方法又进入了连接的操作
                        cleanup();
                        if (state.isAlive()) {
                            eventThread.queueEvent(new WatchedEvent(
                                    Event.EventType.None,
                                    Event.KeeperState.Disconnected,
                                    null));
                        }
                        clientCnxnSocket.updateNow();
                        clientCnxnSocket.updateLastSendAndHeard();
                    }
                }
            }
            //7） 下面是state is not alive的情况
            cleanup();
            clientCnxnSocket.close();//关闭socket
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None,
                        Event.KeeperState.Disconnected, null));
            }
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                    "SendThread exited loop for session: 0x"
                           + Long.toHexString(getSessionId()));
        }

        // 找到读写server，更新rwServerAddress
        // PS: 是目前client只连接了只读的zk server，会不断地调用，更新rwServerAddress
        private void pingRwServer() throws RWServerFoundException, UnknownHostException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);
            LOG.info("Checking server " + addr + " for being r/w." +
                    " Timeout " + pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostName(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();
                br = new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server " +
                        e.getMessage(), e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }
            // 如果ping到的是读写服务器，就抛异常，然后进行重试
            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // save the found address so that it's used during the next
                // connection attempt
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at "
                        + addr.getHostName() + ":" + addr.getPort());
            }
        }

        //socket清理以及通知两个queue失去连接 以及 清理两个队列
        private void cleanup() {
            clientCnxnSocket.cleanup();
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            synchronized (outgoingQueue) {
                for (Packet p : outgoingQueue) {
                    conLossPacket(p);
                }
                outgoingQueue.clear();
            }
        }

        /**
         * Callback invoked by the ClientCnxnSocket once a connection has been
         * established.
         * 
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        // 这个方法是收到了zk server的连接回复后的一些参数设置，以及zk state的状态改变
        // 读取server的connect response后，设置相关参数
        void onConnected(int _negotiatedSessionTimeout, long _sessionId,
                byte[] _sessionPasswd, boolean isRO) throws IOException {
            negotiatedSessionTimeout = _negotiatedSessionTimeout;

            // 服务端认为session超时了（服务器端就会清理与该sessionId相关的数据），则返回给客户端的sessionTimeout时间为0，
            if (negotiatedSessionTimeout <= 0) {
                state = States.CLOSED;

                // 发出一个KeeperState.Expired事件，通知相应的Watcher。
                eventThread.queueEvent(new WatchedEvent(
                        Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.Expired, null));
                // 加入eventOfDeath事件，该事件会结束EventThread线程的循环
                eventThread.queueEventOfDeath();

                String warnInfo;
                warnInfo = "Unable to reconnect to ZooKeeper service, session 0x"
                    + Long.toHexString(sessionId) + " has expired";
                LOG.warn(warnInfo);
                throw new SessionExpiredException(warnInfo);
            }
            // 如果client不允许只读，但是目前是只读
            if (!readOnly && isRO) {
                LOG.error("Read/write client got connected to read-only server");
            }
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();
            // 更新hostProvider循环列表的index
            hostProvider.onConnected();
            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;
            // 根据isRO设置state
            state = (isRO) ? States.CONNECTEDREADONLY : States.CONNECTED;
            // 是否见过读写server
            seenRwServerBefore |= !isRO;
            LOG.info("Session establishment complete on server "
                    + clientCnxnSocket.getRemoteSocketAddress()
                    + ", sessionid = 0x" + Long.toHexString(sessionId)
                    + ", negotiated timeout = " + negotiatedSessionTimeout
                    + (isRO ? " (READ-ONLY mode)" : ""));
            KeeperState eventState = (isRO) ?  KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            // 加入watcherEvent, EventType.None有什么用，估计要看waitingEvents队列的消费者
            eventThread.queueEvent(new WatchedEvent(
                    Watcher.Event.EventType.None,
                    eventState, null));
        }

        //关闭socket
        void close() {
            state = States.CLOSED;
            clientCnxnSocket.wakeupCnxn();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        //是否在验证sasl
        public boolean clientTunneledAuthenticationInProgress() {
            // 1. SASL client is disabled.
            if (!ZooKeeperSaslClient.isEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed == true) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        //发送packet
        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disconnecting client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        sendThread.close();
        eventThread.queueEventOfDeath();
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    private int xid = 1;

    // @VisibleForTesting
    volatile States state = States.NOT_CONNECTED;

    /*
     * getXid() is called externally by ClientCnxnNIO::doIO() when packets are sent from the outgoingQueue to
     * the server. Thus, getXid() must be public.
     */
    synchronized public int getXid() {
        return xid++;
    }

    // 同步获取结果
    public ReplyHeader submitRequest(RequestHeader h, Record request,
            Record response, WatchRegistration watchRegistration)
            throws InterruptedException {
        //生成回复头
        ReplyHeader r = new ReplyHeader();
        Packet packet = queuePacket(h, r, request, response, null, null, null,
                    null, watchRegistration);
        synchronized (packet) {
            //如果packet没有处理完,就一直等着
            while (!packet.finished) {
                //同步获取结果,什么时候唤醒？服务器有响应的时候。。。。
                packet.wait();
            }
        }
        return r;
    }

    public void enableWrite() {
        sendThread.getClientCnxnSocket().enableWrite();
    }

    //调用sendThread往服务器发送请求
    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode) throws IOException {
        // Generate Xid now because it will be sent immediately,
        // by call to sendThread.sendPacket() below.
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    // 请求入队，将请求封装成Packet，加入到outgoingQueue这一发送队列
    // sendThread是这个队列的消费者，主要看run方法
    Packet queuePacket(RequestHeader h, ReplyHeader r, Record request,
            Record response, AsyncCallback cb, String clientPath,
            String serverPath, Object ctx, WatchRegistration watchRegistration)
    {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet. It is
        // generated later at send-time, by an implementation of ClientCnxnSocket::doIO(),
        // where the packet is actually sent.
        synchronized (outgoingQueue) {
            //packet中包含了watchRegistration
            packet = new Packet(h, r, request, response, watchRegistration);
            packet.cb = cb;
            packet.ctx = ctx;
            packet.clientPath = clientPath;
            packet.serverPath = serverPath;
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then
                // mark as closing
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                //放入outgoingQueue即发送队列中
                outgoingQueue.add(packet);
            }
        }
        sendThread.getClientCnxnSocket().wakeupCnxn();
        return packet;
    }

    public void addAuthInfo(String scheme, byte auth[]) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(new RequestHeader(-4, OpCode.auth), null,
                new AuthPacket(0, scheme, auth), null, null, null, null,
                null, null);
    }

    States getState() {
        return state;
    }
}
