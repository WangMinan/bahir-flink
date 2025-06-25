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

import com.influxdb.query.InfluxQLQueryResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.DataPointQueryResultDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author : [wangminan]
 * @description : [InfluxDBSourceDemo]
 */
public class InfluxDBSourceDemo {

    // LOG
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(InfluxDBSourceDemo.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        InfluxDBSource<String> influxDBSource = InfluxDBSource.builder()
                .setInfluxDBUrl("http://10.168.3.103:8086") // http://localhost:8086
                .setInfluxDBToken("MgRgyNCKNoUiVtyylbHVhe0NwemLnHmSgO9HZ7dyZ6NUUU4c9oBkxGN9_wW_75GYN0YObGcYkAF1T2SnElko5Q==")
                .setInfluxDBOrganization("lab418")  // influxdata
                .setInfluxDBBucket("simulation")
                // 内部队列参数 可以不设定用默认值
                .setEnqueueWaitTime(5)
                .setIngestQueueCapacity(10000)
                .setMeasurementName("aa_traj")
                // influxdb连接器的逻辑是基于时间片下发数据 因此您需要指定开始时间 结束时间以及一个分片的时间大小 精度都为纳秒
                .setStartTime(convertInstantToNs(Instant.parse("2025-06-24T00:00:00Z")))
                .setEndTime(convertInstantToNs(Instant.parse("2025-06-25T00:00:00Z")))
                .setSplitDuration(10 * 60 * 1_000_000_000L) // 10MIN 一个shard
                /*
                    您的附加搜索条件 连接器会将其拼接到 WHERE 部分并通过 AND 连接 可以置空
                    这是一个害怕分片内数据过多的不优雅的预留 我们建议您使用数据流本身的 map 和 filter 方法
                 */
                .setWhereCondition(List.of())
                // 设置 influxdb query 返回值的解析器 在连接器框架下 该解析器将数据转换为 datapoint 响应对象
                .setQueryResultDeserializer(
                        (DataPointQueryResultDeserializer) queryResult -> {
                            List<DataPoint> dataPoints = new ArrayList<>();
                            for (InfluxQLQueryResult.Result resultResult : queryResult.getResults()) {
                                for (InfluxQLQueryResult.Series series : resultResult.getSeries()) {
                                    for (InfluxQLQueryResult.Series.Record record : series.getValues()) {
                                        // 类似于 CSV 的列映射 在最深层的 for 循环中您需要指定字段与列的映射关系 在复用较多的场景下您将for循环中的内容单独提出一个方法
                                        DataPoint dataPoint = new DataPoint("aa_traj",
                                                record.getValueByKey("time") != null ?
                                                        Long.parseLong(Objects.requireNonNull(record.getValueByKey("time")).toString()) : null);
                                        dataPoint.addTag("aircraft_id", (String) record.getValueByKey("aircraft_id"));
                                        dataPoint.addTag("sortie_number", (String) record.getValueByKey("sortie_number"));
                                        dataPoint.addField("message_date_time", record.getValueByKey("message_date_time"));
                                        dataPoint.addField("interception_status", record.getValueByKey("interception_status"));
                                        dataPoint.addField("latitude", Double.parseDouble(Objects.requireNonNull(record.getValueByKey("latitude")).toString()));
                                        dataPoints.add(dataPoint);
                                    }
                                }
                            }
                            return dataPoints;
                        }
                )
                // 设置数据点的反序列化器 即最终的输出 将 datapoint 转换为您需要的对象类型
                .setDataPointDeserializer(new InfluxDBDataPointDeserializer<String>() {
                    @Override
                    public String deserialize(DataPoint dataPoint) throws IOException {
                        log.info("dataPoint fetched.");
                        // 真的要转换成对象的时候就在这里用dataPoint.getTag dataPoint.getField就可以
                        return dataPoint.toString();
                    }
                })
                .build();

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDB Source")
                .print();

        env.execute();
    }

    private static Long convertInstantToNs(Instant instant) throws IOException {
        if (instant == null) {
            throw new IOException("Instant cannot be null");
        }
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

    private static Instant convertNsToInstant(Long ns) throws IOException {
        if (ns == null) {
            throw new IOException("Nanoseconds cannot be null");
        }
        long seconds = ns / 1_000_000_000;
        int nanos = (int) (ns % 1_000_000_000);
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
