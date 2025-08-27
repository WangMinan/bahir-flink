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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.http.HttpSourceOptions;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.connectors.http.HttpSourceOptions.INGEST_QUEUE_CAPACITY;

/**
 * @author : [wangminan]
 * @description : Http分片读取器
 */
@Internal
public class HttpSplitReader implements SplitReader<String, HttpSplit>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HttpSplitReader.class);
    private final HttpClient httpClient;
    private final Configuration configuration;
    private HttpSplit split;
    private int emptyQueryCount = 0;
    private boolean splitFinished = false;
    private final FutureCompletingBlockingQueue<List<String>> ingestionQueue;

    public HttpSplitReader(final Configuration configuration) {
        this.configuration = configuration;
        this.httpClient = HttpClient.newHttpClient();
        final int capacity = configuration.get(INGEST_QUEUE_CAPACITY);
        this.ingestionQueue = new FutureCompletingBlockingQueue<>(capacity);
    }

    @Override
    public RecordsWithSplitIds<String> fetch() throws IOException {
        if (this.split == null || this.splitFinished) {
            return new RecordsBySplits.Builder<String>().build();
        }

        // 为每个分片执行一次查询
        executeQueryForSplit();

        final RecordsBySplits.Builder<String> builder = new RecordsBySplits.Builder<>();
        try {
            // 从队列中获取请求结果
            List<String> responses = ingestionQueue.poll();
            if (responses != null && !responses.isEmpty()) {
                for (String response : responses) {
                    builder.add(split.splitId(), response);
                }
            }
        } catch (Exception e) {
            throw new IOException("Error fetching data for split " + split.splitId(), e);
        }

        return finishSplit(builder);
    }

    private RecordsWithSplitIds<String> finishSplit(RecordsBySplits.Builder<String> builder) {
        builder.addFinishedSplit(split.splitId());
        this.splitFinished = true;
        LOG.debug("Finished processing split: {}", split.splitId());
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<HttpSplit> splitsChange) {
        if (splitsChange.splits().isEmpty() || this.split != null) {
            // 一次只处理一个分片
            return;
        }
        this.split = splitsChange.splits().getFirst();
        this.splitFinished = false;
        LOG.debug("Received new split: {}", this.split);
    }

    private void executeQueryForSplit() {
        String url = configuration.get(HttpSourceOptions.REQUEST_URL);
        String method = configuration.get(HttpSourceOptions.REQUEST_METHOD);
        String headersStr = configuration.get(HttpSourceOptions.REQUEST_HEADERS);
        String body = configuration.get(HttpSourceOptions.REQUEST_BODY);
        long timeout = configuration.get(HttpSourceOptions.REQUEST_TIMEOUT_MS);
        int maxRetries = configuration.get(HttpSourceOptions.REQUEST_RETRY_COUNT);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeout));

        if (headersStr != null && !headersStr.isEmpty()) {
            for (String header : headersStr.split(";")) {
                String[] parts = header.split(":", 2);
                if (parts.length == 2) {
                    requestBuilder.header(parts[0].trim(), parts[1].trim());
                }
            }
        }

        if ("POST".equalsIgnoreCase(method)) {
            requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body));
        } else {
            requestBuilder.GET();
        }

        HttpRequest request = requestBuilder.build();

        for (int i = 0; i <= maxRetries; i++) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    LOG.debug("Successfully fetched data from {}", url);
                    ingestionQueue.put(split.splitId().hashCode(), Collections.singletonList(response.body()));
                    return;
                }
                LOG.warn("HTTP request to {} failed with status code {}. Attempt {}/{}", url, response.statusCode(), i + 1, maxRetries + 1);
            } catch (IOException | InterruptedException e) {
                LOG.warn("HTTP request to {} failed. Attempt {}/{}. Error: {}", url, i + 1, maxRetries + 1, e.getMessage());
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        LOG.error("Failed to fetch data from {} after {} retries.", url, maxRetries);
        try {
            ingestionQueue.put(split.splitId().hashCode(), Collections.emptyList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while putting empty list to queue", e);
        }
    }

    @Override
    public void wakeUp() {
        this.ingestionQueue.notifyAvailable();
    }

    @Override
    public void close() {
        // No resources to close
    }
}
