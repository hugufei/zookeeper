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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents critical thread. When there is an uncaught exception thrown by the
 * thread this will exit the system.
 */
// 作为zk危机线程，提供有处理异常的方式，即调用ZooKeeperServerListener.notifyStopping
// 作用：表明了哪些入口的异常可以算作是严重的异常，能够让server关闭的，并且handleException方法完成对ZooKeeperServerListener的调用
public class ZooKeeperCriticalThread extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCriticalThread.class);

    private final ZooKeeperServerListener listener;

    public ZooKeeperCriticalThread(String threadName,ZooKeeperServerListener listener) {
        // 父类是个thread
        super(threadName);
        // 设置监听器实例
        this.listener = listener;
    }

    /**
     * This will be used by the uncaught exception handler and make the system
     * exit.
     * 
     * @param threadName
     *            - thread name
     * @param e
     *            - exception object
     */
    // 处理异常
    // 这里通过观察者模式，当ZooKeeperCriticalThread的实例遇到异常时，调用handleException,会给对应的监听器通知，完成notifyStopping的操作
    @Override
    protected void handleException(String threadName, Throwable e) {
        LOG.error("Severe unrecoverable error, from thread : {}", threadName, e);
        // 观察者模式，调用监听器的对应逻辑
        listener.notifyStopping(threadName, ExitCode.UNEXPECTED_ERROR);
    }
}
