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
package org.apache.flink.streaming.connectors.influxdb.sink2.writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.BATCH_INTERVAL_MS;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.LINE_TXT_FILE_PATH;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.SAVE_LINE_PROTOCOL_TXT_LOCALLY_ON_FAILURE;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.WRITE_BUFFER_SIZE;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.WRITE_DATA_POINT_CHECKPOINT;
import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.getInfluxDBClient;

public class InfluxDBWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriter.class);

    private final int bufferSize;
    private final boolean writeCheckpoint;
    private long lastTimestamp = 0;
    private final List<Point> elements;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;
    private final long batchIntervalMs;
    private final Boolean saveLineTxtLocallyOnFailure;
    private final String lineTxtFilePath;

    // 定时线程池
    private ProcessingTimeService processingTimeService;
    private final Object lock = new Object();
    private volatile boolean closed = false;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer,
            final Configuration configuration) {
        this.schemaSerializer = schemaSerializer;
        this.bufferSize = configuration.get(WRITE_BUFFER_SIZE);
        this.batchIntervalMs = configuration.get(BATCH_INTERVAL_MS);
        this.saveLineTxtLocallyOnFailure = configuration.get(SAVE_LINE_PROTOCOL_TXT_LOCALLY_ON_FAILURE);
        this.lineTxtFilePath = configuration.get(LINE_TXT_FILE_PATH);
        this.elements = new ArrayList<>(this.bufferSize);
        this.writeCheckpoint = configuration.get(WRITE_DATA_POINT_CHECKPOINT);
        this.influxDBClient = getInfluxDBClient(configuration);
    }

    // 为了兼容性保留此方法
    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        // 这个方法保留但为空，不再使用ProcessingTimeService
        this.processingTimeService = processingTimerService;
        if (processingTimerService != null) {
            LOG.debug("ProcessingTimeService is set, scheduling next flush.");
            scheduleNextFlush(); // 调度下一个刷新
        } else {
            LOG.warn("ProcessingTimeService is not set, timer-based flushing will not be used.");
        }
    }

    private void scheduleNextFlush() {
        if (processingTimeService != null) {
            long nextFlushTime = processingTimeService.getCurrentProcessingTime() + batchIntervalMs;
            processingTimeService.registerTimer(nextFlushTime, timestamp -> {
                flushBuffer();
                scheduleNextFlush(); // 递归调度下一次刷新
            });
        }
    }

    // 定时器调用的方法
    private void flushBuffer() {
        try {
            synchronized (lock) {
                if (!closed && !elements.isEmpty()) {
                    LOG.debug("Timer triggered: flushing {} elements", elements.size());
                    writeCurrentElements();
                }
            }
        } catch (Exception e) {
            LOG.error("Error while flushing elements", e);
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        synchronized (lock) {
            if (closed) {
                throw new IOException("Writer is already closed");
            }

            LOG.trace("Adding elements to buffer. Buffer size: {}", this.elements.size());
            this.elements.add(this.schemaSerializer.serialize(in, context));

            if (this.elements.size() >= this.bufferSize) {
                LOG.debug("Buffer size reached preparing to write the elements.");
                this.writeCurrentElements();
            }
            if (context.timestamp() != null) {
                this.lastTimestamp = Math.max(this.lastTimestamp, context.timestamp());
            }
        }
    }

    @Override
    public void flush(boolean flush) {
        if (this.lastTimestamp == 0) return;

        /*
            Thanks to bahir-flink PR 168
            https://github.com/apache/bahir-flink/pull/168/commits/d331fd90b4bf04ad53a605a9c1b61a84d5a86607
         */
        this.writeCurrentElements();
        commit(Collections.singletonList(this.lastTimestamp));
    }

    public void commit(final List<Long> committables) {
        if (this.writeCheckpoint) {
            LOG.debug("A checkpoint is set.");
            Optional<Long> lastTimestamp = Optional.empty();
            if (!committables.isEmpty()) {
                lastTimestamp = Optional.ofNullable(committables.getLast());
            }
            lastTimestamp.ifPresent(this::writeCheckpointDataPoint);
        }
    }

    private void writeCheckpointDataPoint(final Long timestamp) {
        final Point point = new Point("checkpoint")
                .addField("checkpoint", "flink")
                .time(timestamp, WritePrecision.MS);

        writeElementsOf(Collections.singletonList(point));
        LOG.debug("Checkpoint data point write at {}", point.toLineProtocol());
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;

            LOG.debug("Preparing to write the remaining elements in InfluxDB.");
            this.writeCurrentElements();

            LOG.debug("Closing the writer.");
            this.influxDBClient.close();
        }
    }

    private void writeCurrentElements() {
        LOG.debug("Elements remaining: {}", this.elements.size());
        writeElementsOf(this.elements);
        this.elements.clear();
    }

    private void writeElementsOf(List<Point> toWrite) {
        LOG.debug("Writing {} data points to InfluxDB", toWrite.size());
        if (toWrite.isEmpty()) return;

        try (final WriteApi writeApi = this.influxDBClient.makeWriteApi()) {
            writeApi.writePoints(toWrite);
            LOG.debug("Wrote {} data points", toWrite.size());
        } catch (Exception e) {
            LOG.error("Error writing data points to InfluxDB", e);
            // save data points to local txt
            if (saveLineTxtLocallyOnFailure) {
                saveDataPointsInLineProtocol(toWrite);
            }
        }
    }

    private void saveDataPointsInLineProtocol(List<Point> toWrite) {
        LOG.info("Saving data points when encountered failure enabled, writing data points to local txt file at {}", lineTxtFilePath);
        File lineTxtFile = new File(lineTxtFilePath);
        // 检查并创建目录和文件
        if (!lineTxtFile.exists()) {
            try {
                // 检查并创建父目录
                File parentDir = lineTxtFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    if (parentDir.mkdirs()) {
                        LOG.info("Parent directories created for {}", lineTxtFilePath);
                    } else {
                        LOG.error("Failed to create parent directories for {}", lineTxtFilePath);
                        return;
                    }
                }
                if (lineTxtFile.createNewFile()) {
                    LOG.info("Txt file created at {}", lineTxtFilePath);
                } else {
                    LOG.error("Failed to create Txt file at {}", lineTxtFilePath);
                    return;
                }
            } catch (IOException ioException) {
                LOG.error("Error creating Txt file at {}", lineTxtFilePath, ioException);
                return;
            }
        }
        if (!lineTxtFile.canWrite()) {
            LOG.error("Cannot write to Txt file at {}, please check the permissions.", lineTxtFilePath);
            return;
        }
        // 使用FileChannel和FileLock
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(lineTxtFile, "rw");
             FileChannel channel = randomAccessFile.getChannel()) {
            // 尝试获取文件锁
            FileLock lock = null;
            try {
                lock = channel.lock();
                channel.position(channel.size());
                StringBuilder contentBuilder = new StringBuilder();
                for (Point point : toWrite) {
                    contentBuilder.append(point.toLineProtocol())
                            .append(System.lineSeparator());
                }
                // 写入内容
                ByteBuffer buffer = ByteBuffer.wrap(contentBuilder.toString().getBytes(StandardCharsets.UTF_8));
                while (buffer.hasRemaining()) {
                    int write = channel.write(buffer);
                    if (write == 0) {
                        LOG.warn("No bytes were written to the file: {}, possible I/O issue.", lineTxtFilePath);
                        break;
                    }
                }
                // 确保数据被写入磁盘
                channel.force(true);
            } finally {
                // 释放锁
                if (lock != null && lock.isValid()) {
                    lock.release();
                }
            }
        } catch (IOException ex) {
            LOG.error("Error writing to Txt file at {}, some data points will be lost.", lineTxtFilePath, ex);
        }
    }
}
