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
package org.apache.flink.streaming.connectors.influxdb.common;


import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.utils.Arguments;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * InfluxDB data point class.
 *
 * <h3>Elements of line protocol</h3>
 *
 * <pre>
 *
 * measurementName,tagKey=tagValue fieldKey="fieldValue" 1465839830100400200
 * --------------- --------------- --------------------- -------------------
 *      |               |                   |                     |
 * Measurement       Tag set            Field set              Timestamp
 *
 * </pre>
 *
 * <p>{@link InfluxParser} parses line protocol into this data point representation.
 */
public class DataPoint {

    private String measurement;
    private Map<String, String> tags = new HashMap<>();
    private Map<String, Object> fields = new HashMap<>();
    private Long timestamp;

    public DataPoint(final String measurementName, @Nullable final Long timestamp) {
        Arguments.checkNotNull(measurementName, "measurement");
        this.measurement = measurementName;
        this.timestamp = timestamp;
    }

    /**
     * Converts the DataPoint object to {@link Point} object. The default precision for timestamps
     * is in nanoseconds. For more information about timestamp precision please go to <a
     * href=https://docs.influxdata.com/influxdb/cloud/write-data/#timestamp-precision>timestamp-precision</a>
     *
     * @return {@link Point}.
     */
    public Point toPoint() {
        final Point point = new Point(this.measurement);
        point.time(this.timestamp, WritePrecision.NS);
        point.addTags(this.tags);
        point.addFields(this.fields);
        return point;
    }

    /**
     * Adds key and value to field set.
     *
     * @param field Key of field.
     * @param value Value for the field key.
     */
    public void addField(final String field, final Object value) {
        Arguments.checkNonEmpty(field, "fieldName");
        this.fields.put(field, value);
    }

    /**
     * Gets value for a specific field.
     *
     * @param field Key of field.
     * @return value Value for the field key.
     */
    @SuppressWarnings("unchecked")
    public <T> T getField(final String field) {
        Arguments.checkNonEmpty(field, "fieldName");
        return (T) this.fields.getOrDefault(field, null);
    }

    /**
     * Adds key and value to tag set.
     *
     * @param key   Key of tag.
     * @param value Value for the tag key.
     */
    public void addTag(final String key, final String value) {
        Arguments.checkNotNull(key, "tagName");
        this.tags.put(key, value);
    }

    /**
     * Gets value for a specific tag.
     *
     * @param key Key of tag.
     * @return value Value for the tag key.
     */
    public String getTag(final String key) {
        Arguments.checkNotNull(key, "tagName");
        return this.tags.getOrDefault(key, null);
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public String getMeasurement() {
        return this.measurement;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * A point is uniquely identified by the measurement name, tag set, and timestamp. If you submit
     * line protocol with the same measurement, tag set, and timestamp, but with a different field
     * set, the field set becomes the union of the old field set and the new field set, where any
     * conflicts favor the new field set.
     *
     * @param obj: Object to compare to
     * @return Either the object is equal to the data point or not
     * @see <a
     * href="https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/#duplicate-points">
     * Duplicate points </a>
     */
    @Override
    public boolean equals(final Object obj) {

        // If the object is compared with itself then return true
        if (obj == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
        "null instanceof [type]" also returns false */
        if (!(obj instanceof DataPoint point)) {
            return false;
        }

        // typecast o to DataPoint so that we can compare data members

        return point.measurement.equals(this.measurement)
                && point.tags.equals(this.tags)
                && (point.timestamp.equals(this.timestamp));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.measurement, this.fields, this.timestamp);
    }

    @Override
    public String toString() {
        return "DataPoint{" +
                "measurement='" + measurement + '\'' +
                ", tags=" + tags +
                ", fields=" + fields +
                ", timestamp=" + timestamp +
                '}';
    }
}
