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
package org.apache.flink.streaming.connectors.influxdb.source;

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
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumState;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumStateSerializer;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSplitEnumerator;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBRecordEmitter;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSourceReader;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSplitReader;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.DataPointQueryResultDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplitSerializer;

import java.util.List;
import java.util.function.Supplier;

/**
 * The Source implementation of InfluxDB. Please use a {@link InfluxDBSourceBuilder} to construct a
 * {@link InfluxDBSource}. The following example shows how to create an InfluxDBSource emitting
 * records of <code>Long</code> type.
 *
 * <pre>{@code
 * InfluxDBSource<Long> influxDBSource = InfluxBSource.builder()
 * .setDataPointDeserializer(new InfluxDBDeserializer())
 * .build()
 * }</pre>
 *
 * <p>See {@link InfluxDBSourceBuilder} for more details.
 *
 * @param <OUT> the output type of the source.
 */
public final class InfluxDBSource<OUT>
        implements Source<OUT, InfluxDBSplit, InfluxDBSourceEnumState>, ResultTypeQueryable<OUT> {

    private final Configuration configuration;
    private final InfluxDBDataPointDeserializer<OUT> deserializationSchema;
    private final List<String> whereCondition;
    private final String bucket;
    private final String measurement;
    private final long startTime;
    private final long endTime;
    private final long splitDuration;
    private final Boundedness boundedness;
    private final DataPointQueryResultDeserializer queryResultDeserializer;


    InfluxDBSource(
            Configuration configuration,
            InfluxDBDataPointDeserializer<OUT> deserializationSchema,
            String bucket,
            List<String> whereCondition,
            String measurement,
            long startTime,
            long endTime,
            long splitDuration,
            DataPointQueryResultDeserializer queryResultDeserializer) {
        this.configuration = configuration;
        this.deserializationSchema = deserializationSchema;
        this.bucket = bucket;
        this.whereCondition = whereCondition;
        this.measurement = measurement;
        this.startTime = startTime;
        this.endTime = endTime;
        this.splitDuration = splitDuration;
        this.queryResultDeserializer = queryResultDeserializer;
        // 我们在这里用一个常量
        this.boundedness = Boundedness.BOUNDED;
    }

    // builder
    public static <OUT> InfluxDBSourceBuilder<OUT> builder() {
        return new InfluxDBSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, InfluxDBSplit> createReader(SourceReaderContext sourceReaderContext) {
        final Supplier<InfluxDBSplitReader> splitReaderSupplier =
                () -> new InfluxDBSplitReader(configuration, whereCondition, queryResultDeserializer);
        final InfluxDBRecordEmitter<OUT> recordEmitter =
                new InfluxDBRecordEmitter<>(this.deserializationSchema);

        return new InfluxDBSourceReader<>(
                splitReaderSupplier, recordEmitter, this.configuration, sourceReaderContext);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> createEnumerator(
            SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext) {
        return new InfluxDBSplitEnumerator(
                splitEnumeratorContext,
                bucket,
                measurement,
                startTime,
                endTime,
                splitDuration,
                boundedness);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext,
            InfluxDBSourceEnumState enumState) {
        return new InfluxDBSplitEnumerator(
                splitEnumeratorContext,
                bucket,
                measurement,
                startTime,
                endTime,
                splitDuration,
                boundedness,
                enumState.getUnassignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSplit> getSplitSerializer() {
        return new InfluxDBSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new InfluxDBSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }
}
