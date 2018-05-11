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

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AWishabiNginxTimestampExtractor implements TimestampExtractor {
  private static final Logger log = LoggerFactory.getLogger(AWishabiNginxTimestampExtractor.class);

  private DateTimeFormatter dateTime;
  private String regex = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})? (.*?) \\[(.+?)\\] .*";
  private Pattern pattern = Pattern.compile(regex);

  @Override
  public void configure(Map<String, Object> config) {
    dateTime = ISODateTimeFormat.dateTimeParser();
  }

  @Override
  public Long extract(ConnectRecord<?> record) {
    String beaconString = null;
    try {
      Object beacon = record.value();

      if (beacon instanceof byte[]) {
        beaconString = new String((byte[]) beacon);
      } else if (beacon instanceof String) {
        beaconString = (String) beacon;
      } else {
        throw new PartitionException("Error extracting timestamp."
            + " Record is not an instance of String or byte[] for record: " + record);
      }

      Matcher m = pattern.matcher(beaconString);
      m.matches();
      return dateTime.parseMillis(m.group(3));
    } catch (IllegalStateException | IllegalArgumentException e) {
      if (beaconString == null) {
        log.error("Error extracting timestamp from record: " + record + "\nException: ", e);
        return 0L;
      } else {
        log.error("Error extracting timestamp from record: " + record + "\nBeacon = "
            + beaconString + "\nException: ", e);
        return 0L;
      }
    }
  }
}