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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/* Configurations for a InfluxDBSource. */
public final class InfluxDBSourceOptions {

    private InfluxDBSourceOptions() {}

    public static final ConfigOption<Long> ENQUEUE_WAIT_TIME =
            ConfigOptions.key("source.influxDB.timeout.enqueue")
                    .longType()
                    .defaultValue(5L)
                    .withDescription(
                            "The time out in seconds for enqueuing an HTTP request to the queue.");

    public static final ConfigOption<Integer> INGEST_QUEUE_CAPACITY =
            ConfigOptions.key("source.influxDB.queue_capacity.ingest")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of queue that buffers HTTP requests data points before fetching.");

    public static final ConfigOption<String> INFLUXDB_URL =
            ConfigOptions.key("sink.influxDB.client.URL")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB Connection URL.");

    public static final ConfigOption<String> INFLUXDB_USERNAME =
            ConfigOptions.key("sink.influxDB.client.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB username.");

    public static final ConfigOption<String> INFLUXDB_PASSWORD =
            ConfigOptions.key("sink.influxDB.client.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB password.");

    public static final ConfigOption<String> INFLUXDB_TOKEN =
            ConfigOptions.key("sink.influxDB.client.token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB access token.");

    public static final ConfigOption<String> INFLUXDB_BUCKET =
            ConfigOptions.key("sink.influxDB.client.bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB bucket name.");

    public static final ConfigOption<String> INFLUXDB_ORGANIZATION =
            ConfigOptions.key("sink.influxDB.client.organization")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("InfluxDB organization name.");

    public static InfluxDBClient getInfluxDBClient(final Configuration configuration) {
        final String url = configuration.get(INFLUXDB_URL);
        final String username = configuration.get(INFLUXDB_USERNAME);
        final String password = configuration.get(INFLUXDB_PASSWORD);
        final String token = configuration.get(INFLUXDB_TOKEN);
        final String bucket = configuration.get(INFLUXDB_BUCKET);
        final String organization = configuration.get(INFLUXDB_ORGANIZATION);
        InfluxDBClientOptions.Builder builder = InfluxDBClientOptions.builder();
        builder = builder
                .url(url)
                .bucket(bucket)
                .org(organization);
        if (token != null) {
            builder = builder.authenticateToken(token.toCharArray());
        } else if (username != null && password != null) {
            builder = builder.authenticate(username, password.toCharArray());
        }
        final InfluxDBClientOptions influxDBClientOptions = builder.build();
        return InfluxDBClientFactory.create(influxDBClientOptions);
    }
}
