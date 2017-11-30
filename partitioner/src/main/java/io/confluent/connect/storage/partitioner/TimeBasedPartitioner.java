/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.partitioner;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.DataException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.errors.PartitionException;

public class TimeBasedPartitioner<T> extends DefaultPartitioner<T> {
  // Duration of a partition in milliseconds.
  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);
  private long partitionDurationMs;
  private String pathFormat;
  private DateTimeFormatter formatter;
  protected TimestampExtractor timestampExtractor;

  protected void init(
      long partitionDurationMs,
      String pathFormat,
      Locale locale,
      DateTimeZone timeZone,
      Map<String, Object> config
  ) {
    delim = getDirectoryDelimiter(config);
    this.partitionDurationMs = partitionDurationMs;
    this.pathFormat = pathFormat;
    this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
    try {
      partitionFields = newSchemaGenerator(config).newPartitionFields(pathFormat);
      timestampExtractor = newTimestampExtractor(
          (String) config.get(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
      timestampExtractor.configure(config);
    } catch (IllegalArgumentException e) {
      ConfigException ce = new ConfigException(
          PartitionerConfig.PATH_FORMAT_CONFIG,
          pathFormat,
          e.getMessage()
      );
      ce.initCause(e);
      throw ce;
    }
  }

  private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimestamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

  public long getPartitionDurationMs() {
    return partitionDurationMs;
  }

  public String getPathFormat() {
    return pathFormat;
  }

  // public for testing
  public TimestampExtractor getTimestampExtractor() {
    return timestampExtractor;
  }

  @Override
  public void configure(Map<String, Object> config) {
    long partitionDurationMsProp =
        (long) config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    if (partitionDurationMsProp < 0) {
      throw new ConfigException(
          PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
          partitionDurationMsProp,
          "Partition duration needs to be a positive."
      );
    }

    delim = getDirectoryDelimiter(config);
    String pathFormat = (String) config.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    if (pathFormat.equals("") || pathFormat.equals(delim)) {
      throw new ConfigException(
          PartitionerConfig.PATH_FORMAT_CONFIG,
          pathFormat,
          "Path format cannot be empty."
      );
    } else if (delim.equals(pathFormat.substring(pathFormat.length() - delim.length() - 1))) {
      // Delimiter has been added by the user at the end of the path format string. Removing.
      pathFormat = pathFormat.substring(0, pathFormat.length() - delim.length());
    }

    String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
    if (localeString.equals("")) {
      throw new ConfigException(
          PartitionerConfig.LOCALE_CONFIG,
          localeString,
          "Locale cannot be empty."
      );
    }

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    if (timeZoneString.equals("")) {
      throw new ConfigException(
          PartitionerConfig.TIMEZONE_CONFIG,
          timeZoneString,
          "Timezone cannot be empty."
      );
    }

    Locale locale = new Locale(localeString);
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    init(partitionDurationMsProp, pathFormat, locale, timeZone, config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    long timestamp = timestampExtractor.extract(sinkRecord);
    DateTime bucket = new DateTime(
        getPartition(partitionDurationMs, timestamp, formatter.getZone())
    );
    return bucket.toString(formatter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
    Class<? extends SchemaGenerator<T>> generatorClass = null;
    try {
      generatorClass =
          (Class<? extends SchemaGenerator<T>>) config.get(
              PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG
          );
      return generatorClass.getConstructor(Map.class).newInstance(config);
    } catch (ClassCastException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException e) {
      ConfigException ce = new ConfigException("Invalid generator class: " + generatorClass);
      ce.initCause(e);
      throw ce;
    }
  }

  public TimestampExtractor newTimestampExtractor(String extractorClassName) {
    try {
      switch (extractorClassName) {
        case "Wallclock":
        case "Record":
        case "RecordField":
          extractorClassName = "io.confluent.connect.storage.partitioner.TimeBasedPartitioner$"
              + extractorClassName
              + "TimestampExtractor";
          break;
        default:
      }
      Class<?> klass = Class.forName(extractorClassName);
      if (!TimestampExtractor.class.isAssignableFrom(klass)) {
        throw new ConnectException(
            "Class " + extractorClassName + " does not implement TimestampExtractor"
        );
      }
      return (TimestampExtractor) klass.newInstance();
    } catch (ClassNotFoundException
        | ClassCastException
        | IllegalAccessException
        | InstantiationException e) {
      ConfigException ce = new ConfigException(
          "Invalid timestamp extractor: " + extractorClassName
      );
      ce.initCause(e);
      throw ce;
    }
  }

  public static class WallclockTimestampExtractor implements TimestampExtractor {
    @Override
    public void configure(Map<String, Object> config) {}

    @Override
    public Long extract(ConnectRecord<?> record) {
      return Time.SYSTEM.milliseconds();
    }
  }

  public static class RecordTimestampExtractor implements TimestampExtractor {
    @Override
    public void configure(Map<String, Object> config) {}

    @Override
    public Long extract(ConnectRecord<?> record) {
      return record.timestamp();
    }
  }

  public static class RecordFieldTimestampExtractor implements TimestampExtractor {
    private String fieldName;
    private String fieldNameSource;
    private DateTimeFormatter dateTime;

    @Override
    public void configure(Map<String, Object> config) {
      fieldName = (String) config.get(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
      fieldNameSource = (String) config.get(PartitionerConfig.TIMESTAMP_FIELD_SOURCE_CONFIG);
      dateTime = ISODateTimeFormat.dateTimeParser();
    }

    @Override
    public Long extract(ConnectRecord<?> record) {
      Object source;
      if (fieldNameSource.equals("key")) {
        source = record.key();
      } else {
        source = record.value();
      }

      if (source instanceof Struct) {
        Struct struct = (Struct) source;
        Object timestampValue = getNestedFieldValue(struct);
        Schema fieldSchema = getNestedField(record.valueSchema()).schema();

        if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
          return ((Date) timestampValue).getTime();
        }

        switch (fieldSchema.type()) {
          case INT32:
          case INT64:
            return ((Number) timestampValue).longValue();
          case STRING:
            return dateTime.parseMillis((String) timestampValue);
          default:
            log.error(
                "Unsupported type '{}' for user-defined timestamp field.",
                fieldSchema.type().getName()
            );
            throw new PartitionException(
                "Error extracting timestamp from record field: " + fieldName
            );
        }
      } else if (source instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) source;
        Object timestampValue = getNestedFieldValue(map);
        if (timestampValue instanceof Number) {
          return ((Number) timestampValue).longValue();
        } else if (timestampValue instanceof String) {
          return dateTime.parseMillis((String) timestampValue);
        } else if (timestampValue instanceof Date) {
          return ((Date) timestampValue).getTime();
        } else {
          log.error(
              "Unsupported type '{}' for user-defined timestamp field.",
              timestampValue.getClass()
          );
          throw new PartitionException(
              "Error extracting timestamp from record field: " + fieldName
          );
        }
      } else {
        log.error("Value is not of Struct or Map type.");
        throw new PartitionException("Error encoding partition.");
      }
    }

    private Object getField(Struct struct, String fieldName) {
      Object field;
      try {
        field = struct.get(fieldName);
      } catch (DataException e) {
        throw new DataException(
                String.format("The field named '%s' does not exist.", fieldName), e);
      }
      return field;
    }

    private Object getNestedFieldValue(Struct struct) {
      final String[] fieldNames = fieldName.split("\\.");
      Struct tmpStruct = struct;
      Object tmpObject;
      try {
        // Iterate down to final struct
        int i = 0;
        while (i < fieldNames.length - 1) {
          try {
            tmpObject = getField(tmpStruct, fieldNames[i]);
          } catch (DataException e) {
            throw new DataException(
                    String.format("Unable to find nested field '%s'", fieldNames[i]));
          }
          tmpStruct = (Struct) tmpObject;
          i++;
        }
        // Extract from final struct
        tmpObject = getField(tmpStruct, fieldNames[i]);
      } catch (DataException e) {
        throw new DataException(
                String.format("The nested field named '%s' does not exist.", fieldName), e);
      }
      return tmpObject;
    }

    private Object getNestedFieldValue(Map<?, ?> valueMap) {
      final String[] fieldNames = fieldName.split("\\.");
      Map<?, ?> tmpMap = valueMap;
      Object tmpObject;
      try {
        // Iterate down to final map
        int i = 0;
        while (i < fieldNames.length - 1) {
          tmpObject = tmpMap.get(fieldNames[i]);
          if (tmpObject == null) {
            throw new DataException(
                    String.format("Unable to find nested field '%s'", fieldNames[i]));
          }
          tmpMap = (Map<?, ?>) tmpObject;
          i++;
        }
        // Extract from final map
        tmpObject = tmpMap.get(fieldNames[i]);
        if (tmpObject == null) {
          throw new DataException(
                  String.format("Unable to find nested field '%s'", fieldNames[i]));
        }
      } catch (DataException e) {
        throw new DataException(
                String.format("The nested field named '%s' does not exist.", fieldName), e);
      }
      return tmpObject;
    }

    private Field getNestedField(Schema schema) {
      final String[] fieldNames = fieldName.split("\\.");
      if (fieldNames.length == 1) {
        return schema.field(fieldName);
      }
      int i = 0;
      Field tmpField = schema.field(fieldNames[i++]);
      try {
        // Iterate down to final schema
        while (i < fieldNames.length - 1) {
          final String nestedFieldName = fieldNames[i];
          try {
            tmpField = tmpField.schema().field(nestedFieldName);
          } catch (DataException e) {
            throw new DataException(
                    String.format("Unable to find nested field '%s'", nestedFieldName));
          }
          i++;
        }
        // Extract from final schema
        tmpField = tmpField.schema().field(fieldNames[i]);
      } catch (DataException e) {
        throw new DataException(
                String.format("The nested field named '%s' does not exist.", fieldName), e);
      }
      return tmpField;
    }
  }
}
