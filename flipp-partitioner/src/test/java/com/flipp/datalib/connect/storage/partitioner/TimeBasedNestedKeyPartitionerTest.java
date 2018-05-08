package com.flipp.datalib.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TimeBasedNestedKeyPartitionerTest extends StorageSinkTestBase {
  private static final String timeZoneString = "America/New_York";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);

  @Test
  public void testNestedRecordFieldTimestampExtractorFromKey() throws Exception {
    Map<String, Object> config = createConfig("nested.timestamp", "key");

    TimestampExtractor timestampExtractor = new TimeBasedNestedKeyPartitioner.RecordFieldTimestampExtractor();
    timestampExtractor.configure(config);

    long expectedTimestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecordWithNestedTimeField(expectedTimestamp, expectedTimestamp + 100);

    long actualTimestamp = timestampExtractor.extract(sinkRecord);
    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testNestedRecordFieldTimestampExtractorFromValue() throws Exception {
    Map<String, Object> config = createConfig("nested.timestamp", "value");

    TimestampExtractor timestampExtractor = new TimeBasedNestedKeyPartitioner.RecordFieldTimestampExtractor();
    timestampExtractor.configure(config);

    long expectedTimestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecordWithNestedTimeField(expectedTimestamp + 100, expectedTimestamp);

    long actualTimestamp = timestampExtractor.extract(sinkRecord);
    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testNestedRecordFieldTimestampExtractorFromKeyString() throws Exception {
    Map<String, Object> config = createConfig("nested.timestamp", "key");

    TimestampExtractor timestampExtractor = new TimeBasedNestedKeyPartitioner.RecordFieldTimestampExtractor();
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

    TimestampExtractor timestampExtractor = new TimeBasedNestedKeyPartitioner.RecordFieldTimestampExtractor();
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

    config.put(TimeBasedNestedKeyPartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record" +
        (timeFieldName == null ? "" : "Field"));
    config.put(TimeBasedNestedKeyPartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.MINUTES.toMillis(30));
    config.put(TimeBasedNestedKeyPartitionerConfig.PATH_FORMAT_CONFIG, "'aid=flipp'/'dt'=YYYY-MM-dd/'time_slot'=HHmm");
    config.put(TimeBasedNestedKeyPartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
    config.put(TimeBasedNestedKeyPartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());
    config.put(TimeBasedNestedKeyPartitionerConfig.TIMESTAMP_FIELD_SOURCE_CONFIG, timeFieldSource);
    if (timeFieldName != null) {
      config.put(TimeBasedNestedKeyPartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, timeFieldName);
    }
    return config;
  }

  private SinkRecord createSinkRecordWithNestedTimeField(long keyTimestamp, long valueTimestamp) {
    Struct keyRecord = createRecordWithNestedTimeStampField(keyTimestamp);
    Struct valueRecord = createRecordWithNestedTimeStampField(valueTimestamp);
    return new SinkRecord(TOPIC, PARTITION, keyRecord.schema(), keyRecord, valueRecord.schema(), valueRecord, 0L,
        keyTimestamp, TimestampType.CREATE_TIME);
  }

  private SinkRecord createSinkRecordWithNestedTimeFieldString(String keyTimestamp, String valueTimestamp, long timestamp) {
    Struct keyRecord = createRecordWithNestedStringTimeField(keyTimestamp);
    Struct valueRecord = createRecordWithNestedStringTimeField(valueTimestamp);
    return new SinkRecord(TOPIC, PARTITION, keyRecord.schema(), keyRecord, valueRecord.schema(), valueRecord, 0L,
        timestamp, TimestampType.CREATE_TIME);
  }

  private Schema createSchemaWithStringTimestampField() {
    return SchemaBuilder.struct().name("record").version(1)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .field("string", SchemaBuilder.string().defaultValue("abc").build())
        .field("timestamp", Schema.STRING_SCHEMA)
        .build();
  }

  private Struct createRecordWithStringTimestampField(Schema newSchema, String timestamp) {
    return new Struct(newSchema)
        .put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2)
        .put("string", "def")
        .put("timestamp", timestamp);
  }

  private Struct createRecordWithNestedTimeStampField(long timestamp) {
    Schema nestedChildSchema = createSchemaWithTimestampField();
    Schema nestedSchema = SchemaBuilder.struct().field("nested", nestedChildSchema);
    return new Struct(nestedSchema)
        .put("nested", createRecordWithTimestampField(nestedChildSchema, timestamp));
  }

  private Struct createRecordWithNestedStringTimeField(String timestamp) {
    Schema nestedChildSchema = createSchemaWithStringTimestampField();
    Schema nestedSchema = SchemaBuilder.struct().field("nested", nestedChildSchema);
    return new Struct(nestedSchema)
        .put("nested", createRecordWithStringTimestampField(nestedChildSchema, timestamp));
  }
}
