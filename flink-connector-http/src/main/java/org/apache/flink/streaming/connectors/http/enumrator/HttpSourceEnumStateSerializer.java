/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.connectors.http.enumrator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : Http状态序列化器 只要序列化一个寂寞就可以了
 */
@Internal
public class HttpSourceEnumStateSerializer implements SimpleVersionedSerializer<HttpSourceEnumState> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(final HttpSourceEnumState influxDBSourceEnumState) {
        return new byte[0];
    }

    @Override
    public HttpSourceEnumState deserialize(final int i, final byte[] bytes) {
        return new HttpSourceEnumState();
    }
}
