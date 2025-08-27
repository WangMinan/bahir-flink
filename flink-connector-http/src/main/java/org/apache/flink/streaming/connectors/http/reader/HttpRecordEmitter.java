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

package org.apache.flink.streaming.connectors.http.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.streaming.connectors.http.reader.deserializer.HttpResponseBodyDeserializer;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;

import java.io.IOException;

/**
 * @author : [wangminan]
 * @description : Http记录发射器
 */
@Internal
public class HttpRecordEmitter<T> implements RecordEmitter<String, T, HttpSplit> {
    private final HttpResponseBodyDeserializer<T> dataPointDeserializer;

    public HttpRecordEmitter(final HttpResponseBodyDeserializer<T> dataPointDeserializer) {
        this.dataPointDeserializer = dataPointDeserializer;
    }

    @Override
    public void emitRecord(
            final String element, final SourceOutput<T> output, final HttpSplit splitState)
            throws IOException {
        output.collect(this.dataPointDeserializer.deserialize(element));
    }
}
