package com.flipp.datalib.connect.storage.partitioner;

import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class AWishabiTimestampExtractorTest {

  @Test
  public void testNestedRecordFieldTimestampExtractorFromValueString() throws Exception {

    TimestampExtractor timestampExtractor = new AWishabiNginxTimestampExtractor();
    timestampExtractor.configure(new HashMap<String, Object>());

    String valueTimestamp = "2018-05-04T07:12:50-04:00";
    String beacon = "10.0.1.131 45.17.90.40 [2018-05-04T07:12:50-04:00] gid=0A00000A3240EC5A8B05966B026C0CC7 - \"GET /track.gif?aid=editorials&mt=widget&et=view&st=flyer&t=1525432369951&rnd=850730e29cd55e2292374cd970666ad4&targeting_type=none&targetable=false&sid=db90e043acea58dd465048b0a8e92587&client_type=4&module_id=18841&targeted=false&profile_available=false&fsa=39564&channel_id=2382&flyer_type_id=5699&flyer_run_id=337849&flyer_id=1643463&merchant_id=806&premium_merchant=true&in_targeting_profile=false&budget_id=14384 HTTP/1.1\" 200 35 \"-\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 [FBAN/FBIOS;FBAV/169.0.0.50.95;FBBV/104829965;FBDV/iPhone9,2;FBMD/iPhone;FBSN/iOS;FBSV/10.3.3;FBSS/3;FBCR/Sprint;FBID/phone;FBLC/en_US;FBOP/5;FBRV/106137295] FBExtensions/0.1 (transactions-enabled)\" \"app1.e.ana.wishabi.net\"";
    DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
    long expectedTimestamp = parser.parseMillis(valueTimestamp);


    SinkRecord sinkRecord = new SinkRecord("TOPIC", 0, null, null, null, beacon.getBytes(), 0L,
        0L, TimestampType.CREATE_TIME);

    long actualTimestamp = timestampExtractor.extract(sinkRecord);
    assertEquals(expectedTimestamp, actualTimestamp);
  }
}
