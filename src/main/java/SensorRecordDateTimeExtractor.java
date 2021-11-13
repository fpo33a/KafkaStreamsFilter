
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SensorRecordDateTimeExtractor implements TimestampExtractor {

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    //--------------------------------------------------

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousValue) {
        String date = this.getValueByKey("datetime",(String) record.value());
        // System.out.print(">  SensorRecordDateTimeExtractor.extract : date: "+date );
        long dt = 0;
        try {
            Date d = dateFormat.parse(date);
            dt =  d.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            dt = 0;
        }
        // System.out.println(" : long: "+dt + " - key: "+ (String)record.key() );
        return dt;
    }

    //--------------------------------------------------

    // ok this is not the right way to do a json field content extraction but this is fast
    private String getValueByKey(String key, String value) {
        String field = "\"" + key + "\": \"";
        int pos = value.indexOf(field);
        int start = pos + field.length();
        int end = value.indexOf("\"", start);
        String result = value.substring(start, end);
        return result;
    }

    //--------------------------------------------------

}
