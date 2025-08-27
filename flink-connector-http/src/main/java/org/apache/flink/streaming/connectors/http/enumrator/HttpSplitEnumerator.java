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

package org.apache.flink.streaming.connectors.http.enumrator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.http.split.HttpSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author : [wangminan]
 * @description : Http分发器
 */
@Internal
public class HttpSplitEnumerator implements SplitEnumerator<HttpSplit, HttpSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSplitEnumerator.class);
    // 这是一个有界的Http数据源
    private final Boundedness boundedness = Boundedness.BOUNDED;
    // SplitEnumerator的上下文
    private final SplitEnumeratorContext<HttpSplit> context;
    // 存储未分配的splits
    private final List<HttpSplit> unassignedSplits = new ArrayList<>();
    // 等待分配split的读取器
    private final Map<Integer, String> readersAwaitingSplit = new HashMap<>();

    public HttpSplitEnumerator(SplitEnumeratorContext<HttpSplit> context) {
        this.context = checkNotNull(context);
    }

    public HttpSplitEnumerator(SplitEnumeratorContext<HttpSplit> context, List<HttpSplit> unassignedSplits) {
        LOG.warn("Restoring HttpSplitEnumerator with {} unassigned splits, UNEXPECTED.", unassignedSplits.size());
        this.context = checkNotNull(context);
        this.unassignedSplits.addAll(checkNotNull(unassignedSplits));
    }

    @Override
    public void start() {
        // 初始化 unassignedSplits 我们全局只需要一个 split 对于这玩意没办法有分片策略 fuck
        String splitId = "http-split-0";
        unassignedSplits.add(new HttpSplit(splitId));
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
            HttpSplit split = unassignedSplits.removeFirst();
            context.assignSplit(split, subtaskId);
            LOG.debug("Assigned split {} to subtask {}", split, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HttpSplit> splits, int subtaskId) {
        LOG.info("Adding back {} splits from subtask {}", splits.size(), subtaskId);
        unassignedSplits.addAll(splits);

        // 如果当前有读取器在等待分片，尝试重新分配
        if (!readersAwaitingSplit.isEmpty()) {
            assignSplits();
        }
    }

    private void assignSplits() {
        // 为等待的读取器分配分片
        Iterator<Map.Entry<Integer, String>> iterator = readersAwaitingSplit.entrySet().iterator();
        while (iterator.hasNext() && !unassignedSplits.isEmpty()) {
            Map.Entry<Integer, String> entry = iterator.next();
            HttpSplit split = unassignedSplits.removeFirst();
            context.assignSplit(split, entry.getKey());
            iterator.remove();
            LOG.debug("Assigned split {} to waiting subtask {}", split, entry.getKey());
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // 读取器注册时不需要特殊处理
    }

    @Override
    public void close() {
        // 没有需要关闭的资源
        LOG.info("Closing all splits");
    }

    @Override
    public HttpSourceEnumState snapshotState(long checkpointId) {
        return new HttpSourceEnumState(new ArrayList<>(unassignedSplits));
    }
}
