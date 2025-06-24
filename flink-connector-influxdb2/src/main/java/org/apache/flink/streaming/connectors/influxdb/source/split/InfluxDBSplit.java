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
package org.apache.flink.streaming.connectors.influxdb.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serial;
import java.io.Serializable;

/** A {@link SourceSplit} for a InfluxDB split. */
@Internal
public final class InfluxDBSplit implements SourceSplit, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String bucket;
    private final String measurement;  // 测量名称
    private long startTime;      // 查询开始时间戳(us)
    private long endTime;        // 查询结束时间戳(us)
    private long splitDuration;  // 分片数据量大小
    private long currentOffset;  // 当前处理的时间戳偏移量

    public InfluxDBSplit(String splitId, String bucket, String measurement, long startTime, long endTime) {
        this.splitId = splitId;
        this.bucket = bucket;
        this.measurement = measurement;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    // getter和setter方法
    @Override
    public String splitId() {
        return splitId;
    }

    public String getBucket() {
        return bucket;
    }

    public String getMeasurement() {
        return measurement;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
    public long getSplitDuration() {
        return splitDuration;
    }

    public void setSplitDuration(long splitDuration) {
        this.splitDuration = splitDuration;
    }

    public long getCurrentOffset() {
        return currentOffset == 0 ? startTime : currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    // 序列化支持
    @Override
    public String toString() {
        return "InfluxDBSplit{" +
                "splitId='" + splitId + '\'' +
                ", bucket='" + bucket + '\'' +
                ", measurement='" + measurement + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", splitDuration=" + splitDuration +
                ", currentOffset=" + currentOffset +
                '}';
    }
}
