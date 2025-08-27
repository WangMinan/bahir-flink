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

package org.apache.flink.streaming.connectors.http;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.http.reader.deserializer.HttpResponseBodyDeserializer;

import java.io.IOException;

/**
 * @author : [wangminan]
 * @description : HttpSource测试
 */
public class HttpSourceDemo {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HttpSourceDemo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSource<String> httpSource = HttpSource.<String>builder()
                .setUrl("http://10.168.3.101:8080/task/dataAsset/querySortiesByBatchId?batchId=1")
                .setDeserializer(new HttpResponseBodyDeserializer<String>() {
                    @Override
                    public String deserialize(String responseBody) throws IOException {
                        return responseBody;
                    }
                })
                .build();

        env.fromSource(httpSource, WatermarkStrategy.noWatermarks(), "http-source")
                .print();

        env.execute();
    }
}
