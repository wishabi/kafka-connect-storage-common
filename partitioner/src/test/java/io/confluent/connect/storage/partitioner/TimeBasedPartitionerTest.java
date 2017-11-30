/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TimeBasedPartitionerTest extends StorageSinkTestBase {
    private static final String timeZoneString = "America/New_York";
    private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);

    @Test
    public void testNestedRecordFieldTimestampExtractorFromKey() throws Exception {
        Map<String, Object> config = createConfig("nested.timestamp", "key");

        TimestampExtractor timestampExtractor = new TimeBasedPartitioner.RecordFieldTimestampExtractor();
        timestampExtractor.configure(config);

        long expectedTimestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        SinkRecord sinkRecord = createSinkRecordWithNestedTimeField(expectedTimestamp, expectedTimestamp+100);

        long actualTimestamp = timestampExtractor.extract(sinkRecord);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    @Test
    public void testNestedRecordFieldTimestampExtractorFromValue() throws Exception {
        Map<String, Object> config = createConfig("nested.timestamp", "value");

        TimestampExtractor timestampExtractor = new TimeBasedPartitioner.RecordFieldTimestampExtractor();
        timestampExtractor.configure(config);

        long expectedTimestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
        SinkRecord sinkRecord = createSinkRecordWithNestedTimeField(expectedTimestamp+100, expectedTimestamp);

        long actualTimestamp = timestampExtractor.extract(sinkRecord);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    @Test
    public void testNestedRecordFieldTimestampExtractorFromKeyString() throws Exception {
        Map<String, Object> config = createConfig("nested.timestamp", "key");

        TimestampExtractor timestampExtractor = new TimeBasedPartitioner.RecordFieldTimestampExtractor();
        timestampExtractor.configure(config);

        String keyTimestamp = "2017-11-29T19:48:26-05:00";
        String valueTimestamp = "2013-04-21T02:59:59-00:00";

        DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
        long expectedTimestamp = parser.parseMillis(keyTimestamp);
        SinkRecord sinkRecord = createSinkRecordWithNestedTimeFieldString(keyTimestamp, valueTimestamp, expectedTimestamp);

        long actualTimestamp = timestampExtractor.extract(sinkRecord);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    @Test
    public void testNestedRecordFieldTimestampExtractorFromValueString() throws Exception {
        Map<String, Object> config = createConfig("nested.timestamp", "value");

        TimestampExtractor timestampExtractor = new TimeBasedPartitioner.RecordFieldTimestampExtractor();
        timestampExtractor.configure(config);

        String keyTimestamp = "2017-11-29T19:48:26-05:00";
        String valueTimestamp = "2013-04-21T02:59:59-00:00";

        DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
        long expectedTimestamp = parser.parseMillis(valueTimestamp);
        SinkRecord sinkRecord = createSinkRecordWithNestedTimeFieldString(keyTimestamp, valueTimestamp, expectedTimestamp);

        long actualTimestamp = timestampExtractor.extract(sinkRecord);
        assertEquals(expectedTimestamp, actualTimestamp);
    }

    private Map<String, Object> createConfig(String timeFieldName, String timeFieldSource) {
        Map<String, Object> config = new HashMap<>();

        config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record" +
                (timeFieldName == null ? "" : "Field"));
        config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.MINUTES.toMillis(30));
        config.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'aid=flipp'/'dt'=YYYY-MM-dd/'time_slot'=HHmm");
        config.put(PartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
        config.put(PartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());
        config.put(PartitionerConfig.TIMESTAMP_FIELD_SOURCE_CONFIG, timeFieldSource);
        config.put(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG, "io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator");
        if (timeFieldName != null) {
            config.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, timeFieldName);
        }
        return config;
    }

    private SinkRecord createSinkRecordWithNestedTimeField(long keyTimestamp, long valueTimestamp) {
        Struct keyRecord = createRecordWithNestedTimeField(keyTimestamp);
        Struct valueRecord = createRecordWithNestedTimeField(valueTimestamp);
        return new SinkRecord(TOPIC, PARTITION, keyRecord.schema(), keyRecord, valueRecord.schema(), valueRecord, 0L,
                keyTimestamp, TimestampType.CREATE_TIME);
    }

    private SinkRecord createSinkRecordWithNestedTimeFieldString(String keyTimestamp, String valueTimestamp, long timestamp) {
        Struct keyRecord = createRecordWithNestedStringTimeField(keyTimestamp);
        Struct valueRecord = createRecordWithNestedStringTimeField(valueTimestamp);
        return new SinkRecord(TOPIC, PARTITION, keyRecord.schema(), keyRecord, valueRecord.schema(), valueRecord, 0L,
                timestamp, TimestampType.CREATE_TIME);
    }
}
