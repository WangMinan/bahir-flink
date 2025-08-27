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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author : [wangminan]
 * @description : Http源读取器
 */
@Internal
public class HttpSourceReader<OUT> extends SingleThreadMultiplexSourceReaderBase<String, OUT, HttpSplit, HttpSplit> {

    public HttpSourceReader(
            final Supplier<HttpSplitReader> splitReaderSupplier,
            final RecordEmitter<String, OUT, HttpSplit> recordEmitter,
            final Configuration config,
            final SourceReaderContext context) {
        super(splitReaderSupplier::get, recordEmitter, config, context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (this.getNumberOfCurrentlyAssignedSplits() == 0) {
            this.context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(final Map<String, HttpSplit> map) {
        this.context.sendSplitRequest();
    }

    @Override
    protected HttpSplit initializedState(final HttpSplit influxDBSplit) {
        return influxDBSplit;
    }

    @Override
    protected HttpSplit toSplitType(final String s, final HttpSplit influxDBSplitState) {
        return influxDBSplitState;
    }
}
