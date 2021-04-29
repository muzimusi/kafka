/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
// Controller会给集群中的所有Broker（包括它自己所在的Broker）机器发送网络请求。发送请求的目的，是让Broker执行相应的指令。当前，Controller只会向Broker发送三类请求，分别是LeaderAndIsrRequest、StopReplicaRequest和UpdateMetadataRequest。
// Abstract class for all control requests including UpdateMetadataRequest, LeaderAndIsrRequest and StopReplicaRequest（社区越来越倾向于将重要的数据结构源代码从服务器端的core工程移动到客户端的clients工程中）
public abstract class AbstractControlRequest extends AbstractRequest {

    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        protected final int controllerId; // Controller所在的Broker ID。
        protected final int controllerEpoch; // Controller的版本信息。
        protected final long brokerEpoch; // 目标Broker的Epoch。（这两个Epoch字段用于隔离Zombie Controller和Zombie Broker，以保证集群的一致性。）

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
        }

    }

    protected AbstractControlRequest(ApiKeys api, short version) {
        super(api, version);
    }

    public abstract int controllerId();

    public abstract int controllerEpoch();

    public abstract long brokerEpoch();

}
