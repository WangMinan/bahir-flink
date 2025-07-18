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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * InfluxDBSplit的序列化器实现。
 */
@Internal
public final class InfluxDBSplitSerializer implements SimpleVersionedSerializer<InfluxDBSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(final InfluxDBSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            // 写入所有字段
            out.writeUTF(split.splitId());
            out.writeUTF(split.getBucket());
            out.writeUTF(split.getMeasurement());
            out.writeLong(split.getStartTime());
            out.writeLong(split.getEndTime());
            out.writeLong(split.getSplitDuration());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public InfluxDBSplit deserialize(final int version, final byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(inputStream)) {

            // 读取所有字段
            String splitId = in.readUTF();
            String bucket = in.readUTF();
            String measurement = in.readUTF();
            long startTime = in.readLong();
            long endTime = in.readLong();
            long splitDuration = in.readLong();

            // 创建并返回InfluxDBSplit实例
            InfluxDBSplit split = new InfluxDBSplit(splitId, bucket, measurement, startTime, endTime);
            split.setSplitDuration(splitDuration);
            return split;
        }
    }
}
