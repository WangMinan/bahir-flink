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
package org.apache.flink.streaming.connectors.influxdb.sink2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.influxdb.sink2.writer.InfluxDBSchemaSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink2.writer.InfluxDBWriter;

import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.BATCH_INTERVAL_MS;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_BUCKET;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_ORGANIZATION;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_PASSWORD;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_TOKEN;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_URL;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.INFLUXDB_USERNAME;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.WRITE_BUFFER_SIZE;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.WRITE_DATA_POINT_CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The @builder class for {@link org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSink} to make it easier for the users to construct a {@link
 * org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSink}.
 *
 * <p>The following example shows the minimum setup to create a InfluxDBSink that uses the Long
 * values from a former operator and sends it to an InfluxDB instance.
 *
 * <pre>{@code
 * InfluxDBSink<Long> influxDBSink = InfluxDBSink.builder()
 * .setInfluxDBSchemaSerializer(new InfluxDBSerializer())
 * .setInfluxDBUrl(getUrl())
 * .setInfluxDBUsername(getUsername())
 * .setInfluxDBPassword(getPassword())
 * .setInfluxDBBucket(getBucket())
 * .setInfluxDBOrganization(getOrg())
 * .build();
 * }</pre>
 *
 * <p>To specify the batch size that has a significant influence on performance, one can call {@link
 * #setWriteBufferSize(int)}.
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * InfluxDBSource.
 */
public final class InfluxDBSinkBuilder<IN> {
    private InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private String influxDBUrl;
    private String influxDBUsername;
    private String influxDBPassword;
    private String influxDBToken;
    private String bucketName;
    private String organizationName;
    private final Configuration configuration;

    InfluxDBSinkBuilder() {
        this.influxDBUrl = null;
        this.influxDBUsername = null;
        this.influxDBPassword = null;
        this.influxDBToken = null;
        this.bucketName = null;
        this.organizationName = null;
        this.influxDBSchemaSerializer = null;
        this.configuration = new Configuration();
    }

    /**
     * Sets the InfluxDB url.
     *
     * @param influxDBUrl the url of the InfluxDB instance to send data to.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBUrl(final String influxDBUrl) {
        this.influxDBUrl = influxDBUrl;
        this.configuration.setString(INFLUXDB_URL, checkNotNull(influxDBUrl));
        return this;
    }

    /**
     * Sets the InfluxDB user name.
     *
     * @param influxDBUsername the user name of the InfluxDB instance.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBUsername(final String influxDBUsername) {
        this.influxDBUsername = influxDBUsername;
        this.configuration.setString(INFLUXDB_USERNAME, checkNotNull(influxDBUsername));
        return this;
    }

    /**
     * Sets the InfluxDB password.
     *
     * @param influxDBPassword the password of the InfluxDB instance.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBPassword(final String influxDBPassword) {
        this.influxDBPassword = influxDBPassword;
        this.configuration.setString(INFLUXDB_PASSWORD, checkNotNull(influxDBPassword));
        return this;
    }

    /**
     * Sets the InfluxDB token.
     *
     * @param influxDBToken the token of the InfluxDB instance.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBToken(final String influxDBToken) {
        this.influxDBToken = influxDBToken;
        this.configuration.setString(INFLUXDB_TOKEN, checkNotNull(influxDBToken));
        return this;
    }

    /**
     * Sets the InfluxDB bucket name.
     *
     * @param bucketName the bucket name of the InfluxDB instance to store the data in.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBBucket(final String bucketName) {
        this.bucketName = bucketName;
        this.configuration.setString(INFLUXDB_BUCKET, checkNotNull(bucketName));
        return this;
    }

    /**
     * Sets the InfluxDB organization name.
     *
     * @param organizationName the organization name of the InfluxDB instance.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBOrganization(final String organizationName) {
        this.organizationName = organizationName;
        this.configuration.setString(INFLUXDB_ORGANIZATION, checkNotNull(organizationName));
        return this;
    }

    /**
     * Sets the {@link InfluxDBSchemaSerializer serializer} of the input type IN for the
     * InfluxDBSink.
     *
     * @param influxDBSchemaSerializer the serializer for the input type.
     * @return this InfluxDBSourceBuilder.
     */
    public <T extends IN> InfluxDBSinkBuilder<T> setInfluxDBSchemaSerializer(
            final InfluxDBSchemaSerializer<T> influxDBSchemaSerializer) {
        checkNotNull(influxDBSchemaSerializer);
        final InfluxDBSinkBuilder<T> sinkBuilder = (InfluxDBSinkBuilder<T>) this;
        sinkBuilder.influxDBSchemaSerializer = influxDBSchemaSerializer;
        return sinkBuilder;
    }

    /**
     * Sets if the InfluxDBSink should write checkpoint data points to InfluxDB.
     *
     * @param shouldWrite boolean if checkpoint should be written.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> addCheckpointDataPoint(final boolean shouldWrite) {
        this.configuration.setBoolean(WRITE_DATA_POINT_CHECKPOINT, shouldWrite);
        return this;
    }

    /**
     * Sets the buffer size of the {@link InfluxDBWriter}. This also determines the number of {@link
     * com.influxdb.client.write.Point} send to the InfluxDB instance per request.
     *
     * @param bufferSize size of the buffer.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setWriteBufferSize(final int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("The buffer size should be greater than 0.");
        }
        this.configuration.setInteger(WRITE_BUFFER_SIZE, bufferSize);
        return this;
    }

    /**
     * 设置批处理间隔时间，用于控制数据刷新到InfluxDB的频率。
     * 即使缓冲区未满，也会在指定的时间间隔后将数据刷新到InfluxDB。
     *
     * @param batchIntervalMs 批处理间隔时间（毫秒）
     * @return 当前InfluxDBSinkBuilder实例
     */
    public InfluxDBSinkBuilder<IN> setBatchIntervalMs(final long batchIntervalMs) {
        if (batchIntervalMs <= 0) {
            throw new IllegalArgumentException("The batch interval must be greater than 0 milliseconds.");
        }
        this.configuration.setLong(BATCH_INTERVAL_MS, batchIntervalMs);
        return this;
    }

    /**
     * Build the {@link org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSink}.
     *
     * @return a InfluxDBSink with the settings made for this builder.
     */
    public InfluxDBSink<IN> build() {
        this.sanityCheck();
        return new InfluxDBSink<>(this.influxDBSchemaSerializer, this.configuration);
    }

    // ------------- private helpers  --------------

    /** Checks if the SchemaSerializer and the influxDBConfig are not null and set. */
    private void sanityCheck() {
        // Check required settings.
        checkNotNull(this.influxDBUrl, "The InfluxDB URL is required but not provided.");
        // check that either username/password or token is provided for authentication
        checkArgument(
                this.influxDBToken != null
                        || (this.influxDBUsername != null && this.influxDBPassword != null),
                "Either the InfluxDB username and password or InfluxDB token are required but neither provided"
        );
        // check that both username/password and token are not both provided for authentication
        checkArgument(
                ! (this.influxDBToken != null
                        && (this.influxDBUsername != null || this.influxDBPassword != null)),
                "Either the InfluxDB username and password or InfluxDB token are required but both provided"
        );
        checkNotNull(this.bucketName, "The Bucket name is required but not provided.");
        checkNotNull(this.organizationName, "The Organization name is required but not provided.");
        checkNotNull(
                this.influxDBSchemaSerializer,
                "Serialization schema is required but not provided.");
    }
}
