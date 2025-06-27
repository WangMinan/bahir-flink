/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except OUT compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to OUT writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.DataPointQueryResultDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_BUCKET;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_ORGANIZATION;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_PASSWORD;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_TOKEN;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_URL;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_USERNAME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The @builder class for {@link InfluxDBSource} to make it easier for the users to construct a
 * {@link InfluxDBSource}.
 *
 * <p>The following example shows the minimum setup to create a InfluxDBSource that reads the Long
 * values from a line protocol source.
 *
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * InfluxDBSource.
 */
public final class InfluxDBSourceBuilder<OUT> {

    private InfluxDBDataPointDeserializer<OUT> deserializationSchema;
    private final Configuration configuration;
    private List<String> whereCondition;
    private String influxDBUrl;
    private String influxDBToken;
    private String bucketName;
    private String organizationName;
    private String measurementName;
    private long startTime;
    private long stopTime;
    private long splitDuration;
    private DataPointQueryResultDeserializer queryResultDeserializer;
    private Boundedness boundedness;

    InfluxDBSourceBuilder() {
        this.influxDBUrl = null;
        this.influxDBToken = null;
        this.bucketName = null;
        this.deserializationSchema = null;
        this.organizationName = null;
        this.whereCondition = new ArrayList<>();
        this.measurementName = null;
        this.configuration = new Configuration();
        this.startTime = 0L; // Default start time
        this.stopTime = Long.MAX_VALUE; // Default end time
        this.splitDuration = 60 * 60 * 1_000_000_000L; // Default split duration (1 hour in nanoseconds)
        this.queryResultDeserializer = null; // Default deserializer
        this.boundedness = Boundedness.BOUNDED; // Default boundedness
    }

    public InfluxDBSourceBuilder<OUT> setStartTime(final long startTime) {
        this.startTime = startTime;
        this.configuration.set(InfluxDBSourceOptions.START_TIME, checkNotNull(startTime));
        return this;
    }

    public InfluxDBSourceBuilder<OUT> setEndTime(final long endTime) {
        this.stopTime = endTime;
        this.configuration.set(InfluxDBSourceOptions.END_TIME, checkNotNull(endTime));
        return this;
    }

    public InfluxDBSourceBuilder<OUT> setSplitDuration(final long splitDuration) {
        this.splitDuration = splitDuration;
        this.configuration.set(InfluxDBSourceOptions.SPLIT_DURATION, checkNotNull(splitDuration));
        return this;
    }


    public InfluxDBSourceBuilder<OUT> setMeasurementName(final String measurementName) {
        this.measurementName = measurementName;
        this.configuration.set(InfluxDBSourceOptions.MEASUREMENT_NAME, checkNotNull(measurementName));
        return this;
    }


    /**
     * Sets the query Flux to use for this InfluxDBSource.
     *
     * @param whereCondition the query Flux to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setWhereCondition(List<String> whereCondition) {
        this.whereCondition = whereCondition;
        return this;
    }

    /**
     * Sets the InfluxDB url.
     *
     * @param influxDBUrl the url of the InfluxDB instance to send data to.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setInfluxDBUrl(final String influxDBUrl) {
        this.influxDBUrl = influxDBUrl;
        this.configuration.set(INFLUXDB_URL, checkNotNull(influxDBUrl));
        return this;
    }

    /**
     * Sets the InfluxDB token.
     *
     * @param influxDBToken the token of the InfluxDB instance.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setInfluxDBToken(final String influxDBToken) {
        this.influxDBToken = influxDBToken;
        this.configuration.set(INFLUXDB_TOKEN, checkNotNull(influxDBToken));
        return this;
    }

    /**
     * Sets the InfluxDB bucket name.
     *
     * @param bucketName the bucket name of the InfluxDB instance to store the data OUT.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setInfluxDBBucket(final String bucketName) {
        this.bucketName = bucketName;
        this.configuration.set(INFLUXDB_BUCKET, checkNotNull(bucketName));
        return this;
    }

    /**
     * Sets the InfluxDB organization name.
     *
     * @param organizationName the organization name of the InfluxDB instance.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setInfluxDBOrganization(final String organizationName) {
        this.organizationName = organizationName;
        this.configuration.set(INFLUXDB_ORGANIZATION, checkNotNull(organizationName));
        return this;
    }

    /**
     * Sets the {@link InfluxDBDataPointDeserializer deserializer} of the {@link
     * org.apache.flink.streaming.connectors.influxdb.common.DataPoint DataPoint} for the
     * InfluxDBSource.
     *
     * @param dataPointDeserializer the deserializer for InfluxDB {@link
     *                              org.apache.flink.streaming.connectors.influxdb.common.DataPoint DataPoint}.
     * @return this InfluxDBSourceBuilder.
     */
    public <T extends OUT> InfluxDBSourceBuilder<T> setDataPointDeserializer(
            final InfluxDBDataPointDeserializer<T> dataPointDeserializer) {
        checkNotNull(dataPointDeserializer);
        final InfluxDBSourceBuilder<T> sourceBuilder = (InfluxDBSourceBuilder<T>) this;
        sourceBuilder.deserializationSchema = dataPointDeserializer;
        return sourceBuilder;
    }

    /**
     * Sets the enqueue wait time, i.e., the time out of this InfluxDBSource.
     *
     * @param timeOut the enqueue wait time to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setEnqueueWaitTime(final long timeOut) {
        this.configuration.set(InfluxDBSourceOptions.ENQUEUE_WAIT_TIME, timeOut);
        return this;
    }

    /**
     * Sets the ingest queue capacity of this InfluxDBSource.
     *
     * @param capacity the capacity to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setIngestQueueCapacity(final int capacity) {
        this.configuration.set(InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY, capacity);
        return this;
    }

    /**
     * Sets the query result deserializer for the InfluxDBSource.
     *
     * @param queryResultDeserializer the deserializer to use for this InfluxDBSource.
     * @return this InfluxDBSourceBuilder.
     */
    public InfluxDBSourceBuilder<OUT> setQueryResultDeserializer(
            final DataPointQueryResultDeserializer queryResultDeserializer) {
        this.queryResultDeserializer = queryResultDeserializer;
        return this;
    }

    public InfluxDBSourceBuilder<OUT> setBoundedness(final Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }

    /**
     * Build the {@link InfluxDBSource}.
     *
     * @return a InfluxDBSource with the settings made for this builder.
     */
    public InfluxDBSource<OUT> build() {
        this.sanityCheck();
        return new InfluxDBSource<>(configuration, deserializationSchema, bucketName,
                whereCondition, measurementName, startTime, stopTime, splitDuration,
                queryResultDeserializer, boundedness);
    }

    // ------------- private helpers  --------------

    private void sanityCheck() {
        checkNotNull(
                this.deserializationSchema, "Deserialization schema is required but not provided.");
        checkNotNull(
                this.influxDBToken, "InfluxDB token is required but not provided."
        );
        checkNotNull(this.bucketName, "The Bucket name is required but not provided.");
        checkNotNull(this.organizationName, "The Organization name is required but not provided.");
        checkNotNull(
                this.influxDBUrl, "The InfluxDB URL is required but not provided.");
        checkNotNull(this.whereCondition, "The where condition is required but not provided.");
        checkArgument(
                this.startTime < this.stopTime,
                "The start time must be less than the stop time. Provided start time: "
                        + this.startTime
                        + ", stop time: "
                        + this.stopTime);
        checkArgument(
                this.splitDuration > 0,
                "The split duration must be greater than 0. Provided split duration: "
                        + this.splitDuration);
        checkArgument(
                this.startTime >= 0,
                "The start time must be greater than or equal to 0. Provided start time: "
                        + this.startTime);
        checkArgument(
                this.boundedness == Boundedness.BOUNDED || this.boundedness == Boundedness.CONTINUOUS_UNBOUNDED,
                "The boundedness must be either BOUNDED or CONTINUOUS_UNBOUNDED. Provided boundedness: "
                        + this.boundedness);
        checkNotNull(
                this.queryResultDeserializer,
                "The query result deserializer is required but not provided.");
        checkNotNull(
                this.measurementName, "The measurement name is required but not provided.");
        checkNotNull(
                this.influxDBUrl, "The InfluxDB URL is required but not provided.");
        checkNotNull(
                this.bucketName, "The InfluxDB bucket name is required but not provided.");
        checkNotNull(
                this.organizationName,
                "The InfluxDB organization name is required but not provided.");

    }
}
