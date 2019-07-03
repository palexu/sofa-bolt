/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.remoting;

/*
 额，似乎只是提供了startup、shutdown等接口， 没啥实际用处。。
    就是让需要启动的一些组件， 拥有相同的启动、关闭方法。

下面这些组件都是有启动、关闭流程的， 所以需要extends这个组件

RpcClient (com.alipay.remoting)
RpcTaskScanner (com.alipay.remoting.rpc)
DefaultConnectionManager (com.alipay.remoting)
ReconnectManager (com.alipay.remoting)
AbstractRemotingServer (com.alipay.remoting)
DefaultConnectionMonitor (com.alipay.remoting)

> 所以实际上主要就是规范下格式。

而不是用于子类之间的复用， 就像马桶和电饭煲都有开关，但是马桶不能替换电饭煲拿来煮饭。 只是说他们要发挥自己的功能时， 需要去点一下按钮。
 */
/**RpcTaskScanner
 * @author chengyi (mark.lx@antfin.com) 2018-11-05 14:43
 */
public abstract class AbstractLifeCycle implements LifeCycle {

    private volatile boolean isStarted = false;

    @Override
    public void startup() throws LifeCycleException {
        if (!isStarted) {
            isStarted = true;
            return;
        }

        throw new LifeCycleException("this component has started");
    }

    @Override
    public void shutdown() throws LifeCycleException {
        if (isStarted) {
            isStarted = false;
            return;
        }

        throw new LifeCycleException("this component has closed");
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }
}
