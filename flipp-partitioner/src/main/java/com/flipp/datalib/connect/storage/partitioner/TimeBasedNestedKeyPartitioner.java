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

package com.flipp.datalib.connect.storage.partitioner;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class TimeBasedNestedKeyPartitioner<T> extends TimeBasedPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(TimeBasedNestedKeyPartitioner.class);

  @Override
  public TimestampExtractor newTimestampExtractor(String extractorClassName) {
    try {
      switch (extractorClassName) {
        case "Wallclock":
        case "Record":
        case "RecordField":
          extractorClassName = "io.confluent.connect.storage.partitioner.TimeBasedNestedKeyPartitioner$"
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

  public static class RecordFieldTimestampExtractor extends TimeBasedPartitioner.RecordFieldTimestampExtractor {
    private String fieldName;
    private String fieldNameSource;
    private DateTimeFormatter dateTime;

    @Override
    public void configure(Map<String, Object> config) {
      fieldName = (String) config.get(TimeBasedNestedKeyPartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
      fieldNameSource = (String) config.get(TimeBasedNestedKeyPartitionerConfig.TIMESTAMP_FIELD_SOURCE_CONFIG);
      dateTime = ISODateTimeFormat.dateTimeParser();
    }

    @Override
    public Long extract(ConnectRecord<?> record) {
      Object source;
      Schema sourceSchema;
      if (fieldNameSource.equals("key")) {
        source = record.key();
        sourceSchema = record.keySchema();
      } else {
        source = record.value();
        sourceSchema = record.valueSchema();
      }

      if (source instanceof Struct) {
        Struct struct = (Struct) source;
        Object timestampValue = getNestedFieldValue(struct);
        Schema fieldSchema = getNestedField(sourceSchema).schema();

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
