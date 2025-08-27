/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.http;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.http.enumrator.HttpSourceEnumState;
import org.apache.flink.streaming.connectors.http.enumrator.HttpSourceEnumStateSerializer;
import org.apache.flink.streaming.connectors.http.enumrator.HttpSplitEnumerator;
import org.apache.flink.streaming.connectors.http.reader.HttpRecordEmitter;
import org.apache.flink.streaming.connectors.http.reader.HttpSourceReader;
import org.apache.flink.streaming.connectors.http.reader.HttpSplitReader;
import org.apache.flink.streaming.connectors.http.reader.deserializer.HttpResponseBodyDeserializer;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;
import org.apache.flink.streaming.connectors.http.split.HttpSplitSerializer;

import java.util.function.Supplier;

/**
 * @author : [wangminan]
 * @description : Http源
 */
public final class HttpSource <OUT>
        implements Source<OUT, HttpSplit, HttpSourceEnumState>, ResultTypeQueryable<OUT> {

    private final Configuration configuration;
    private final HttpResponseBodyDeserializer<OUT> deserializer;

    public HttpSource(Configuration configuration,
                      HttpResponseBodyDeserializer<OUT> deserializer) {
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    // builder
    public static <OUT> HttpSourceBuilder<OUT> builder() {
        return new HttpSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpSourceEnumState> createEnumerator(
            SplitEnumeratorContext<HttpSplit> splitEnumeratorContext) {
        return new HttpSplitEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> splitEnumeratorContext,
            HttpSourceEnumState httpSourceEnumState) {
        return new HttpSplitEnumerator(splitEnumeratorContext, httpSourceEnumState.getUnassignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<HttpSplit> getSplitSerializer() {
        return new HttpSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<HttpSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new HttpSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<OUT, HttpSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        final Supplier<HttpSplitReader> splitReaderSupplier = () -> new HttpSplitReader(configuration);
        final HttpRecordEmitter<OUT> recordEmitter = new HttpRecordEmitter<>(this.deserializer);
        return new HttpSourceReader<>(splitReaderSupplier, recordEmitter, this.configuration, sourceReaderContext);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.deserializer.getProducedType();
    }
}
