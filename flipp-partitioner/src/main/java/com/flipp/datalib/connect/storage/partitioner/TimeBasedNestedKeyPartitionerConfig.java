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
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;

public class TimeBasedNestedKeyPartitionerConfig extends PartitionerConfig {
  public static final String TIMESTAMP_FIELD_SOURCE_CONFIG = "timestamp.source";
  public static final String TIMESTAMP_FIELD_SOURCE_DOC =
      "The source of the record field to be used as timestamp "
      + "by the timestamp extractor, either key or value.";
  public static final String TIMESTAMP_FIELD_SOURCE_DEFAULT = "value";
  public static final String TIMESTAMP_FIELD_SOURCE_DISPLAY =
      "Source of the Record Field for Timestamp Extractor";

  public static ConfigDef newConfigDef(ConfigDef.Recommender partitionerClassRecommender) {
    ConfigDef configDef = new ConfigDef();
    {
      // Define Partitioner configuration group
      final String group = "Partitioner";
      int orderInGroup = 0;

      configDef.define(PARTITIONER_CLASS_CONFIG,
          Type.CLASS,
          PARTITIONER_CLASS_DEFAULT,
          Importance.HIGH,
          PARTITIONER_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          PARTITIONER_CLASS_DISPLAY,
          Arrays.asList(
              PARTITION_FIELD_NAME_CONFIG,
              PARTITION_DURATION_MS_CONFIG,
              PATH_FORMAT_CONFIG,
              LOCALE_CONFIG,
              TIMEZONE_CONFIG
          ),
          partitionerClassRecommender);

      configDef.define(PARTITION_FIELD_NAME_CONFIG,
          Type.STRING,
          PARTITION_FIELD_NAME_DEFAULT,
          Importance.MEDIUM,
          PARTITION_FIELD_NAME_DOC,
          group,
          ++orderInGroup,
          Width.NONE,
          PARTITION_FIELD_NAME_DISPLAY,
          new PartitionerClassDependentsRecommender());

      configDef.define(PARTITION_DURATION_MS_CONFIG,
          Type.LONG,
          PARTITION_DURATION_MS_DEFAULT,
          Importance.MEDIUM,
          PARTITION_DURATION_MS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          PARTITION_DURATION_MS_DISPLAY,
          new PartitionerClassDependentsRecommender());

      configDef.define(PATH_FORMAT_CONFIG,
          Type.STRING,
          PATH_FORMAT_DEFAULT,
          Importance.MEDIUM,
          PATH_FORMAT_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          PATH_FORMAT_DISPLAY,
          new PartitionerClassDependentsRecommender());

      configDef.define(LOCALE_CONFIG,
          Type.STRING,
          LOCALE_DEFAULT,
          Importance.MEDIUM,
          LOCALE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          LOCALE_DISPLAY,
          new PartitionerClassDependentsRecommender());

      configDef.define(TIMEZONE_CONFIG,
          Type.STRING,
          TIMEZONE_DEFAULT,
          Importance.MEDIUM,
          TIMEZONE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          TIMEZONE_DISPLAY,
          new PartitionerClassDependentsRecommender());

      configDef.define(TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
          Type.STRING,
          TIMESTAMP_EXTRACTOR_CLASS_DEFAULT,
          Importance.MEDIUM,
          TIMESTAMP_EXTRACTOR_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          TIMESTAMP_EXTRACTOR_CLASS_DISPLAY);

      configDef.define(TIMESTAMP_FIELD_NAME_CONFIG,
          Type.STRING,
          TIMESTAMP_FIELD_NAME_DEFAULT,
          Importance.MEDIUM,
          TIMESTAMP_FIELD_NAME_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          TIMESTAMP_FIELD_NAME_DISPLAY);

      configDef.define(TIMESTAMP_FIELD_SOURCE_CONFIG,
          Type.STRING,
          TIMESTAMP_FIELD_SOURCE_DEFAULT,
          Importance.MEDIUM,
          TIMESTAMP_FIELD_SOURCE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          TIMESTAMP_FIELD_SOURCE_DISPLAY);
    }

    return configDef;
  }

  public TimeBasedNestedKeyPartitionerConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }
}
