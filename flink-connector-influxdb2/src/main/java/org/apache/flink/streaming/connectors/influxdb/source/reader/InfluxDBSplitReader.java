/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional debugrmation
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
package org.apache.flink.streaming.connectors.influxdb.source.reader;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxQLQueryApi;
import com.influxdb.client.domain.InfluxQLQuery;
import com.influxdb.query.InfluxQLQueryResult;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.DataPointQueryResultDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY;


/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@link DataPoint}.
 */
@Internal
public final class InfluxDBSplitReader implements SplitReader<DataPoint, InfluxDBSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSplitReader.class);

    private final InfluxDBClient influxDBClient;
    private final List<String> whereCondition; // 基础查询模板
    private final DataPointQueryResultDeserializer queryResultDeserializer;
    private int emptyQueryCount = 0;
    private static final int MAX_EMPTY_QUERY_ATTEMPTS = 3;


    private final FutureCompletingBlockingQueue<List<DataPoint>> ingestionQueue;
    private InfluxDBSplit split;

    public InfluxDBSplitReader(final Configuration configuration, List<String> whereCondition,
                               DataPointQueryResultDeserializer queryResultDeserializer, String url, String token, String org) {
        final int capacity = configuration.get(INGEST_QUEUE_CAPACITY);
        this.ingestionQueue = new FutureCompletingBlockingQueue<>(capacity);

        // 直接使用传入的参数构建客户端，不再依赖 getInfluxDBClient
        this.influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org);

        this.whereCondition = whereCondition;
        this.queryResultDeserializer = queryResultDeserializer;
    }

    @Override
    public RecordsWithSplitIds<DataPoint> fetch() throws IOException {
        if (this.split == null) {
            return new RecordsBySplits.Builder<DataPoint>().build();
        }

        final RecordsBySplits.Builder<DataPoint> builder = new RecordsBySplits.Builder<>();

        // 检查是否已处理完当前分片
        if (split.getCurrentOffset() >= split.getEndTime()) {
            return finishSplit();
        }

        // 尝试从队列中获取数据点
        List<DataPoint> dataPoints = this.ingestionQueue.poll();

        if (dataPoints == null || dataPoints.isEmpty()) {
            // 如果队列为空且当前分片未处理完，执行查询
            if (split.getCurrentOffset() < split.getEndTime()) {
                executeQueryForSplit();
                dataPoints = this.ingestionQueue.poll();
            }

            // 执行查询后仍无数据，可能是该时间范围内没有数据
            if (dataPoints == null || dataPoints.isEmpty()) {
                emptyQueryCount++;
                if (emptyQueryCount >= MAX_EMPTY_QUERY_ATTEMPTS) {
                    // 连续多次查询无数据，认为分片已处理完毕
                    LOG.debug("No more data in split {} after {} attempts", split.splitId(), MAX_EMPTY_QUERY_ATTEMPTS);
                    return finishSplit();
                }
                // 移动时间窗口继续查询
                long nextOffset = split.getCurrentOffset() + split.getSplitDuration() / MAX_EMPTY_QUERY_ATTEMPTS;
                nextOffset = Math.min(nextOffset, split.getEndTime());
                split.setCurrentOffset(nextOffset);
                return builder.build();
            }
        }

        // 重置空查询计数器
        emptyQueryCount = 0;

        // 添加记录到结果
        for (DataPoint dataPoint : dataPoints) {
            builder.add(split.splitId(), dataPoint);
        }

        // 更新当前处理的偏移量为最后一个数据点的时间戳+1（确保下次不重复查询）
        long lastTimestamp = dataPoints.stream()
                .mapToLong(DataPoint::getTimestamp)
                .max()
                .orElse(split.getCurrentOffset());

        // 确保偏移量至少前进一点，避免原地踏步
        split.setCurrentOffset(Math.max(lastTimestamp + 1, split.getCurrentOffset() + 1));

        // 检查是否处理完毕
        if (split.getCurrentOffset() >= split.getEndTime()) {
            builder.addFinishedSplit(split.splitId());
        }

        return builder.build();
    }

    private RecordsWithSplitIds<DataPoint> finishSplit() {
        RecordsBySplits.Builder<DataPoint> builder = new RecordsBySplits.Builder<>();
        builder.addFinishedSplit(split.splitId());
        LOG.debug("Finished processing split: {}", split.splitId());
        // 清除当前分片引用
        split = null;
        emptyQueryCount = 0;
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<InfluxDBSplit> splitsChange) {
        if (splitsChange.splits().isEmpty()) {
            return;
        }

        // 停止处理当前分片
        if (this.split != null) {
            LOG.debug("Switching from split {} to new split", this.split.splitId());
        }

        this.split = splitsChange.splits().getFirst();
        LOG.debug("Received new split: {}", this.split);

        // 执行新分片的查询
        executeQueryForSplit();
    }

    private void executeQueryForSplit() {
        // 为当前分片构建InfluxQL查询
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT * FROM ")
                .append(split.getMeasurement())
                .append(" WHERE time >= ")
                .append(split.getCurrentOffset())
                .append(" AND time < ")  // 注意添加时间单位us
                .append(Math.min(split.getCurrentOffset() + split.getSplitDuration(), split.getEndTime()));

        // 添加where条件
        if (whereCondition != null && !whereCondition.isEmpty()) {
            queryBuilder
                    .append(" AND ")
                    .append(String.join(" AND ", whereCondition));
        }

        // 按时间排序确保结果有序
        queryBuilder.append(" ORDER BY time ASC");

        InfluxQLQueryApi queryApi = influxDBClient.getInfluxQLQueryApi();
        LOG.debug("Executing query: {}", queryBuilder);
        InfluxQLQuery influxQLQuery = new InfluxQLQuery(queryBuilder.toString(), split.getBucket());

        try {
            InfluxQLQueryResult queryResult = queryApi.query(influxQLQuery.setAcceptHeader(InfluxQLQuery.AcceptHeader.CSV));
            List<DataPoint> dataPoints = queryResultDeserializer.deserialize(queryResult);
            LOG.debug("Query returned {} data points", dataPoints.size());
            this.ingestionQueue.put(split.splitId().hashCode(), dataPoints);
        } catch (Exception e) {
            LOG.error("Error executing query: {}", e.getMessage(), e);
            // 放入空列表避免阻塞
            try {
                this.ingestionQueue.put(split.splitId().hashCode(), Collections.emptyList());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while putting empty list to queue", ie);
            }
        }
    }

    @Override
    public void wakeUp() {
        this.ingestionQueue.notifyAvailable();
    }

    @Override
    public void close() {
        if (this.influxDBClient != null) {
            try {
                this.influxDBClient.close();
            } catch (final Exception e) {
                throw new RuntimeException("Failed to close InfluxDB client", e);
            }
        }
    }

    // ---------------- private helper class --------------------

    private static class InfluxDBSplitRecords implements RecordsWithSplitIds<DataPoint> {
        private final List<DataPoint> records;
        private Iterator<DataPoint> recordIterator;
        private final String splitId;

        private InfluxDBSplitRecords(final String splitId) {
            this.splitId = splitId;
            this.records = new ArrayList<>();
        }

        private boolean addAll(final List<DataPoint> records) {
            return this.records.addAll(records);
        }

        private void prepareForRead() {
            this.recordIterator = this.records.iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (this.recordIterator.hasNext()) {
                return this.splitId;
            }
            return null;
        }

        @Override
        @Nullable
        public DataPoint nextRecordFromSplit() {
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
