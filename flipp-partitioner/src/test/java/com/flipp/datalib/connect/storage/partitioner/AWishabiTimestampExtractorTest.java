package com.flipp.datalib.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AWishabiTimestampExtractorTest extends StorageSinkTestBase {
  private static final String timeZoneString = "America/New_York";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);



  @Test
  public void testNestedRecordFieldTimestampExtractorFromValueString() throws Exception {

    TimestampExtractor timestampExtractor = new AWishabiNginxTimestampExtractor();
    timestampExtractor.configure(new HashMap<String, Object>());

    String valueTimestamp = "2017-11-21T11:26:33-05:00";
    String beacon = "68.71.31.46 68.71.31.46 [2017-11-21T11:26:33-05:00] gid=CB00000AB953145A5A463317023D79B7 - \"GET /track.gif?repeat=0&merchant_id=234&flyer_id=1425106&postal_code=L3C7C1&analytics_payload=budget_id%3A11610%7Ccmt%3A1%7Cauction_uuid%3A806be883ea654e6882c7c9eaffb2c30d&sid=3108e946-6dc7-404b-bd70-1f205eac2626&t=ev&session_id=812e0d14-19de-4005-b773-2dca19163f0f&platform_device_id=1e9fb181-e56c-4ee2-8e80-f5c2ec21dd48&flyer_type_id=102&system_version=6.0&device_platform=Android&rnd=7b7166bb-73c4-449d-b99a-2ab23651de31&source_action=NULL&allow_push=true&merchant=Walmart&cached_lon=-79.2773505&locale=en_CA&last_listing=null&account_guid=e0f7a5b4-0b76-476d-852d-9460a2002649&sequence_number=7&system_model=LG-D852&tracking_enabled=0&flyer_run_id=279316&app_version=7.1.2&flipp_premium_merchant=1&reachability=wi-fi&cached_lat=43.1624908&channel_id=119&aid=flipp HTTP/1.1\" 200 35 \"-\" \"Dalvik/2.1.0 (Linux; U; Android 6.0; LG-D852 Build/MRA58K)\"";

    DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
    long expectedTimestamp = parser.parseMillis(valueTimestamp);


    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, null, beacon.getBytes(), 0L,
            0L, TimestampType.CREATE_TIME);

    long actualTimestamp = timestampExtractor.extract(sinkRecord);
    assertEquals(expectedTimestamp, actualTimestamp);
  }
}
