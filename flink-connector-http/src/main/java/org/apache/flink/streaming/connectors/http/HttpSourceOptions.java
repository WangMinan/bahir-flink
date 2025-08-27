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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author : [wangminan]
 * @description : Http源Options
 */
public class HttpSourceOptions {
    private HttpSourceOptions() {
    }

    public static final ConfigOption<Integer> INGEST_QUEUE_CAPACITY =
            ConfigOptions.key("source.http.queue_capacity.ingest")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of queue that buffers HTTP requests data before fetching.");

    public static final ConfigOption<String> REQUEST_URL =
            ConfigOptions.key("source.http.request.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The URL to send HTTP requests to.");

    public static final ConfigOption<String> REQUEST_METHOD =
            ConfigOptions.key("source.http.request.method")
                    .stringType()
                    .defaultValue("GET")
                    .withDescription("The HTTP request method, e.g., GET, POST.");

    public static final ConfigOption<String> REQUEST_HEADERS =
            ConfigOptions.key("source.http.request.headers")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The HTTP request headers, formatted as 'Key1:Value1;Key2:Value2'.");

    public static final ConfigOption<String> REQUEST_BODY =
            ConfigOptions.key("source.http.request.body")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The HTTP request body, used for POST requests.");

    public static final ConfigOption<Long> REQUEST_TIMEOUT_MS =
            ConfigOptions.key("source.http.request.timeout.ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("The HTTP request timeout in milliseconds.");

    public static final ConfigOption<Integer> REQUEST_RETRY_COUNT =
            ConfigOptions.key("source.http.request.retry.count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The number of times to retry a failed HTTP request.");
}
