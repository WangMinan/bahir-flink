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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.http.reader.deserializer.HttpResponseBodyDeserializer;

/**
 * @author : [wangminan]
 * @description : Http源构建器
 */
public class HttpSourceBuilder<OUT> {
    private final Configuration configuration;
    private HttpResponseBodyDeserializer<OUT> deserializer;
    private String url;
    private String method = "GET";
    private String headers = "";
    private String body = "";
    private int queueCapacity = 1000;
    private long requestTimeoutMs = 5000L;
    private int maxRetries = 3;

    public HttpSourceBuilder() {
        this.configuration = new Configuration();
    }

    public HttpSourceBuilder<OUT> setDeserializer(HttpResponseBodyDeserializer<OUT> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public HttpSourceBuilder<OUT> setUrl(String url) {
        this.url = url;
        this.configuration.set(HttpSourceOptions.REQUEST_URL, url);
        return this;
    }

    public HttpSourceBuilder<OUT> setMethod(String method) {
        this.method = method;
        this.configuration.set(HttpSourceOptions.REQUEST_METHOD, method);
        return this;
    }

    public HttpSourceBuilder<OUT> setHeaders(String headers) {
        this.headers = headers;
        this.configuration.set(HttpSourceOptions.REQUEST_HEADERS, headers);
        return this;
    }

    public HttpSourceBuilder<OUT> setBody(String body) {
        this.body = body;
        this.configuration.set(HttpSourceOptions.REQUEST_BODY, body);
        return this;
    }

    public HttpSourceBuilder<OUT> setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        this.configuration.set(HttpSourceOptions.INGEST_QUEUE_CAPACITY, queueCapacity);
        return this;
    }

    public HttpSourceBuilder<OUT> setRequestTimeoutMs(Long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.configuration.set(HttpSourceOptions.REQUEST_TIMEOUT_MS, requestTimeoutMs);
        return this;
    }

    public HttpSourceBuilder<OUT> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        this.configuration.set(HttpSourceOptions.REQUEST_RETRY_COUNT, maxRetries);
        return this;
    }

    public HttpSource<OUT> build() {
        if (this.url == null || this.url.isEmpty()) {
            throw new IllegalArgumentException("Request URL must be provided.");
        }
        if (this.deserializer == null) {
            throw new IllegalArgumentException("HttpResponseBodyDeserializer must be provided.");
        }
        return new HttpSource<>(this.configuration, this.deserializer);
    }
}
