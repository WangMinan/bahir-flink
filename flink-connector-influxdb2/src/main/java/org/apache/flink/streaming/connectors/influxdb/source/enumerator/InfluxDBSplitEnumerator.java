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
package org.apache.flink.streaming.connectors.influxdb.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The enumerator class for InfluxDB source.
 */
public class InfluxDBSplitEnumerator implements SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSplitEnumerator.class);

    private final SplitEnumeratorContext<InfluxDBSplit> context;
    private final String bucket;
    private final String measurement;
    private final long startTime;
    private final long endTime;
    private final long splitDuration;
    private final Boundedness boundedness;

    // 存储未分配的splits
    private final List<InfluxDBSplit> unassignedSplits = new ArrayList<>();
    // 等待分配split的读取器
    private final Map<Integer, String> readersAwaitingSplit = new HashMap<>();

    public InfluxDBSplitEnumerator(
            SplitEnumeratorContext<InfluxDBSplit> context,
            String bucket,
            String measurement,
            long startTime,
            long endTime,
            long splitDuration,
            Boundedness boundedness) {
        this.context = checkNotNull(context);
        this.bucket = checkNotNull(bucket);
        this.measurement = measurement;
        this.startTime = startTime;
        this.endTime = endTime;
        this.splitDuration = splitDuration;
        this.boundedness = boundedness;
    }

    public InfluxDBSplitEnumerator(
            SplitEnumeratorContext<InfluxDBSplit> context,
            String bucket,
            String measurement,
            long startTime,
            long endTime,
            long splitDuration,
            Boundedness boundedness,
            List<InfluxDBSplit> unassignedSplits) {
        this(context, bucket, measurement, startTime, endTime, splitDuration, boundedness);
        this.unassignedSplits.addAll(unassignedSplits);
    }

    @Override
    public void start() {
        // 为有界流一次性创建所有分片
        if (boundedness == Boundedness.BOUNDED) {
            createTimeBasedSplits();
        } else {
            // 对于无界流，定期创建新的分片
            long currentTimeMillis = System.currentTimeMillis();
            // 创建过去时间范围的分片
            createSplitsForTimeRange(startTime, Math.min(endTime, currentTimeMillis));

            // 定期创建新的分片（对于实时数据）
            context.callAsync(
                    this::discoverNewSplits,
                    this::processNewSplits,
                    60000, // 每分钟检查一次新数据
                    30000); // 初始延迟30秒
        }
    }

    private List<InfluxDBSplit> discoverNewSplits() {
        long currentTimeMillis = System.currentTimeMillis();
        List<InfluxDBSplit> newSplits = new ArrayList<>();

        // 获取上次处理的最新时间
        long lastProcessedTime = unassignedSplits.isEmpty() ?
                startTime :
                unassignedSplits.stream()
                        .mapToLong(InfluxDBSplit::getEndTime)
                        .max()
                        .orElse(startTime);

        // 只为新的时间范围创建分片
        if (lastProcessedTime < currentTimeMillis) {
            long nextEndTime = Math.min(endTime, currentTimeMillis);
            newSplits.addAll(createSplitsForTimeRange(lastProcessedTime, nextEndTime));
        }

        return newSplits;
    }

    private void processNewSplits(List<InfluxDBSplit> newSplits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to discover new splits", error);
            return;
        }

        if (!newSplits.isEmpty()) {
            LOG.info("Discovered {} new splits", newSplits.size());
            unassignedSplits.addAll(newSplits);
            assignSplits();
        }
    }

    private void createTimeBasedSplits() {
        unassignedSplits.addAll(createSplitsForTimeRange(startTime, endTime));
    }

    private List<InfluxDBSplit> createSplitsForTimeRange(long rangeStart, long rangeEnd) {
        List<InfluxDBSplit> splits = new ArrayList<>();

        // 根据splitDuration将时间范围分割成多个小区间
        for (long start = rangeStart; start < rangeEnd; start += splitDuration) {
            long end = Math.min(start + splitDuration, rangeEnd);
            String splitId = measurement + "-" + start + "-" + end;
            InfluxDBSplit influxDBSplit = new InfluxDBSplit(splitId, bucket, measurement, start, end);
            influxDBSplit.setStartTime(start);
            influxDBSplit.setEndTime(end);
            influxDBSplit.setSplitDuration(splitDuration);
            splits.add(influxDBSplit);
        }

        LOG.info("Created {} splits for time range [{}, {}]",
                splits.size(), rangeStart, rangeEnd);
        return splits;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (unassignedSplits.isEmpty()) {
            if (boundedness == Boundedness.BOUNDED &&
                readersAwaitingSplit.isEmpty() &&
                context.registeredReaders().containsKey(subtaskId)) {
                // 有界流且没有更多分片，发送完成信号
                context.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}, signaling completion", subtaskId);
            } else {
                // 添加到等待列表
                readersAwaitingSplit.put(subtaskId, requesterHostname);
                LOG.info("No splits available for subtask {}, adding to waiting list", subtaskId);
            }
        } else {
            // 分配一个分片
            InfluxDBSplit split = unassignedSplits.removeFirst();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split {} to subtask {}", split, subtaskId);
        }
    }

    private void assignSplits() {
        // 为等待的读取器分配分片
        Iterator<Map.Entry<Integer, String>> iterator = readersAwaitingSplit.entrySet().iterator();
        while (iterator.hasNext() && !unassignedSplits.isEmpty()) {
            Map.Entry<Integer, String> entry = iterator.next();
            InfluxDBSplit split = unassignedSplits.removeFirst();
            context.assignSplit(split, entry.getKey());
            iterator.remove();
            LOG.info("Assigned split {} to waiting subtask {}", split, entry.getKey());
        }
    }

    @Override
    public void addSplitsBack(List<InfluxDBSplit> splits, int subtaskId) {
        LOG.info("Adding back {} splits from subtask {}", splits.size(), subtaskId);
        unassignedSplits.addAll(splits);

        // 如果当前有读取器在等待分片，尝试重新分配
        if (!readersAwaitingSplit.isEmpty()) {
            assignSplits();
        }
    }

    @Override
    public InfluxDBSourceEnumState snapshotState(long checkpointId) {
        return new InfluxDBSourceEnumState(new ArrayList<>(unassignedSplits));
    }

    @Override
    public void addReader(int subtaskId) {
        // 读取器注册时不需要特殊处理
    }

    @Override
    public void close() {
        // 没有需要关闭的资源
    }
}
