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

import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;

public class TimeBasedNestedKeyPartitionerConfig extends PartitionerConfig {
  public static final String TIMESTAMP_FIELD_SOURCE_CONFIG = "timestamp.source";
  public static final String TIMESTAMP_FIELD_SOURCE_DOC =
      "The source of the record field to be used as timestamp by the timestamp extractor, either key or value.";
  public static final String TIMESTAMP_FIELD_SOURCE_DEFAULT = "value";
  public static final String TIMESTAMP_FIELD_SOURCE_DISPLAY = "Source of the Record Field for Timestamp Extractor";

  static {
    {
      // Define Partitioner configuration group
      final String group = "Partitioner";
      int orderInGroup = 0;

      CONFIG_DEF.define(PARTITIONER_CLASS_CONFIG,
          ConfigDef.Type.CLASS,
          PARTITIONER_CLASS_DEFAULT,
          ConfigDef.Importance.HIGH,
          PARTITIONER_CLASS_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          PARTITIONER_CLASS_DISPLAY,
          Arrays.asList(
              PARTITION_FIELD_NAME_CONFIG,
              PARTITION_DURATION_MS_CONFIG,
              PATH_FORMAT_CONFIG,
              LOCALE_CONFIG,
              TIMEZONE_CONFIG,
              SCHEMA_GENERATOR_CLASS_CONFIG
          ));

      CONFIG_DEF.define(SCHEMA_GENERATOR_CLASS_CONFIG,
          ConfigDef.Type.CLASS,
          ConfigDef.Importance.HIGH,
          SCHEMA_GENERATOR_CLASS_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          SCHEMA_GENERATOR_CLASS_DISPLAY);

      CONFIG_DEF.define(PARTITION_FIELD_NAME_CONFIG,
          ConfigDef.Type.STRING,
          PARTITION_FIELD_NAME_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          PARTITION_FIELD_NAME_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.NONE,
          PARTITION_FIELD_NAME_DISPLAY);

      CONFIG_DEF.define(PARTITION_DURATION_MS_CONFIG,
          ConfigDef.Type.LONG,
          PARTITION_DURATION_MS_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          PARTITION_DURATION_MS_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          PARTITION_DURATION_MS_DISPLAY);

      CONFIG_DEF.define(PATH_FORMAT_CONFIG,
          ConfigDef.Type.STRING,
          PATH_FORMAT_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          PATH_FORMAT_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          PATH_FORMAT_DISPLAY);

      CONFIG_DEF.define(LOCALE_CONFIG,
          ConfigDef.Type.STRING,
          LOCALE_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          LOCALE_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          LOCALE_DISPLAY);

      CONFIG_DEF.define(TIMEZONE_CONFIG,
          ConfigDef.Type.STRING,
          TIMEZONE_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          TIMEZONE_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          TIMEZONE_DISPLAY);

      CONFIG_DEF.define(TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
          ConfigDef.Type.STRING,
          TIMESTAMP_EXTRACTOR_CLASS_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          TIMESTAMP_EXTRACTOR_CLASS_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          TIMESTAMP_EXTRACTOR_CLASS_DISPLAY);

      CONFIG_DEF.define(TIMESTAMP_FIELD_NAME_CONFIG,
          ConfigDef.Type.STRING,
          TIMESTAMP_FIELD_NAME_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          TIMESTAMP_FIELD_NAME_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          TIMESTAMP_FIELD_NAME_DISPLAY);

      CONFIG_DEF.define(TIMESTAMP_FIELD_SOURCE_CONFIG,
          ConfigDef.Type.STRING,
          TIMESTAMP_FIELD_SOURCE_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          TIMESTAMP_FIELD_SOURCE_DOC,
          group,
          ++orderInGroup,
          ConfigDef.Width.LONG,
          TIMESTAMP_FIELD_SOURCE_DISPLAY);
    }
  }

  public TimeBasedNestedKeyPartitionerConfig(Map<String, String> props) {
    super(props);
  }
}
