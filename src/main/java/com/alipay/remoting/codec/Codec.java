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
package com.alipay.remoting.codec;

import io.netty.channel.ChannelHandler;

/**
 * Codec interface.
 *
 * @author chengyi (mark.lx@antfin.com) 2018-06-20 21:07
 */
public interface Codec {

    /*
    notes
     下面这两个负责了所有的编码解码工作
     ProtocolCodeBasedDecoder
     ProtocolCodeBasedEncoder
     而Codec主要就是对这两个进行管理， 然后在 RpcServer / RpcClient 中pipeLine 使用时更加方便。
     */

    /**
     * Create an encoder instance.
     *
     * @return new encoder instance
     */
    ChannelHandler newEncoder();

    /**
     * Create an decoder instance.
     *
     * @return new decoder instance
     */
    ChannelHandler newDecoder();
}
