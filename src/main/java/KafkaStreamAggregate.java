/*

TEST Kafka Stream aggregation using hopping window based on business date ( see SensorRecordDateTimeExtractor )

// create an input topic
C:\frank\apache-kafka-2.4.1\bin\windows>kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic test
Created topic test.

// check content of input topic ( generated by uncommenting some code to put some data )
C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test
Processed a total of 0 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
{ "SensorId": "Key0", "datetime": "2021/11/13 14:24:24","SensorValue": 1}
{ "SensorId": "Key1", "datetime": "2021/11/13 14:24:26","SensorValue": 101}
{ "SensorId": "Key2", "datetime": "2021/11/13 14:24:26","SensorValue": 201}
{ "SensorId": "Key3", "datetime": "2021/11/13 14:24:26","SensorValue": 302}
{ "SensorId": "Key4", "datetime": "2021/11/13 14:24:26","SensorValue": 404}
{ "SensorId": "Key0", "datetime": "2021/11/13 14:24:26","SensorValue": 9}
{ "SensorId": "Key1", "datetime": "2021/11/13 14:24:27","SensorValue": 104}
{ "SensorId": "Key2", "datetime": "2021/11/13 14:24:27","SensorValue": 208}
{ "SensorId": "Key3", "datetime": "2021/11/13 14:24:27","SensorValue": 310}
{ "SensorId": "Key4", "datetime": "2021/11/13 14:24:27","SensorValue": 408}
{ "SensorId": "Key0", "datetime": "2021/11/13 14:24:27","SensorValue": 3}
{ "SensorId": "Key1", "datetime": "2021/11/13 14:24:28","SensorValue": 104}
{ "SensorId": "Key2", "datetime": "2021/11/13 14:24:28","SensorValue": 203}
{ "SensorId": "Key3", "datetime": "2021/11/13 14:24:28","SensorValue": 301}
{ "SensorId": "Key4", "datetime": "2021/11/13 14:24:28","SensorValue": 404}
{ "SensorId": "Key0", "datetime": "2021/11/13 14:24:28","SensorValue": 7}
{ "SensorId": "Key1", "datetime": "2021/11/13 14:24:29","SensorValue": 106}
{ "SensorId": "Key2", "datetime": "2021/11/13 14:24:29","SensorValue": 203}
{ "SensorId": "Key3", "datetime": "2021/11/13 14:24:29","SensorValue": 305}
{ "SensorId": "Key4", "datetime": "2021/11/13 14:24:29","SensorValue": 401}
Processed a total of 20 messages
Terminer le programme de commandes (O/N) ? o

// chec Output results : ( key is time window ( so record key @ start window epoch / end window epoch

2021/11/13 14:52:37>Starting data generator thread ...
2021/11/13 14:52:37>Initializing stream ...
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
2021/11/14 10:31:12>Starting ...
{ "key": "Key0", "window": "Window{startMs=1636809855000, endMs=1636809865000}", "nbele": 1, "total": 1, "mean": 1.0, "stddev": 0.0, "values": [1] }
{ "key": "Key0", "window": "Window{startMs=1636809860000, endMs=1636809870000}", "nbele": 4, "total": 20, "mean": 5.0, "stddev": 3.1622776601683795, "values": [1,9,3,7] }
{ "key": "Key0", "window": "Window{startMs=1636809865000, endMs=1636809875000}", "nbele": 3, "total": 19, "mean": 6.333333333333333, "stddev": 2.494438257849294, "values": [9,3,7] }
{ "key": "Key1", "window": "Window{startMs=1636809860000, endMs=1636809870000}", "nbele": 4, "total": 415, "mean": 103.75, "stddev": 1.7853571071357126, "values": [101,104,104,106] }
{ "key": "Key1", "window": "Window{startMs=1636809865000, endMs=1636809875000}", "nbele": 4, "total": 415, "mean": 103.75, "stddev": 1.7853571071357126, "values": [101,104,104,106] }
{ "key": "Key2", "window": "Window{startMs=1636809860000, endMs=1636809870000}", "nbele": 4, "total": 815, "mean": 203.75, "stddev": 2.5860201081971503, "values": [201,208,203,203] }
{ "key": "Key2", "window": "Window{startMs=1636809865000, endMs=1636809875000}", "nbele": 4, "total": 815, "mean": 203.75, "stddev": 2.5860201081971503, "values": [201,208,203,203] }
{ "key": "Key3", "window": "Window{startMs=1636809860000, endMs=1636809870000}", "nbele": 4, "total": 1218, "mean": 304.5, "stddev": 3.5, "values": [302,310,301,305] }
{ "key": "Key3", "window": "Window{startMs=1636809865000, endMs=1636809875000}", "nbele": 4, "total": 1218, "mean": 304.5, "stddev": 3.5, "values": [302,310,301,305] }
{ "key": "Key4", "window": "Window{startMs=1636809860000, endMs=1636809870000}", "nbele": 4, "total": 1617, "mean": 404.25, "stddev": 2.48746859276655, "values": [404,408,404,401] }
{ "key": "Key4", "window": "Window{startMs=1636809865000, endMs=1636809875000}", "nbele": 4, "total": 1617, "mean": 404.25, "stddev": 2.48746859276655, "values": [404,408,404,401] }


 */

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;



import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreamAggregate {

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private final String bootstrapServer = "localhost:9092";
    private final String applicationId = "testAggregateBaseOnBusinessDate";
    private final String topicName = "test";
    private boolean verbose = true;

    //-------------------------------------------------------------------


    public static void main(String[] args) {
        KafkaStreamAggregate test = new KafkaStreamAggregate();

        test.aggregate (args);
        return;
    }

    //-------------------------------------------------------------------

    private void print(String data) {
        if (this.verbose) System.out.println(data);
    }

    //-------------------------------------------------------------------

    private void aggregate(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,  UUID.randomUUID().toString() ) ; //this.applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SensorRecordDateTimeExtractor.class.getName()); // to extract business date from record
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // generate some data into input topic
        this.print(dateFormat.format(new Date()) + ">Starting data generator thread ...");

/* UNCOMMENT to generate some data
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.init(this.verbose, this.bootstrapServer, this.topicName);
        Thread dataGeneratorThread = new Thread(dataGenerator);
        dataGeneratorThread.start();
*/
        this.print(dateFormat.format(new Date()) + ">Initializing stream ...");
        final StreamsBuilder builder = new StreamsBuilder();

        Serializer<SensorAggregator> serializer = new Serializer<SensorAggregator>() {
            @Override
            public byte[] serialize(String s, SensorAggregator sensorAggregator) {
                return sensorAggregator.serializeValues();
            }
        } ;
        Deserializer<SensorAggregator> deserializer = new Deserializer<SensorAggregator>() {
            @Override
            public SensorAggregator deserialize(String s, byte[] bytes) {
                SensorAggregator sensorAggregator = new SensorAggregator();
                sensorAggregator.deserializeValues(new String(bytes));
                return  (sensorAggregator);
            }
        } ;

        KTable<Windowed<String>, SensorAggregator> aggregatedStream = builder.stream(this.topicName, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                //Add a 10 second window and slide the hopping time window in 5 second steps
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10).toMillis()).advanceBy(Duration.ofSeconds(5).toMillis()))
                .aggregate(SensorAggregator::new,
                           (aggKey, newValue, aggValue) -> {
                            aggValue.setData(newValue);
                            return aggValue;
                            },
                            Materialized.with(Serdes.String(), Serdes.serdeFrom(serializer, deserializer)
                           )
                );

        // convert the windowed <key>, sensorAggregator into a more usable <string (key),string (json of aggregated values)> stream
        KStream<String, String> convertedStream = aggregatedStream.toStream()
                .map((Windowed<String> key, SensorAggregator value) -> {
                    // System.out.println(">   map : " + key   + " : NB ELE : " +  value.getNbElement() + " : TOTAL : " + value.getTotal() + " : MEAN : " + value.getMean() + " : STDDEV : " + value.getStandardDeviation()+ " : VALUES : "+value.dump());
                    String jsonResult = "{ \"key\": \""+key.key() + "\", \"window\": \""+key.window() + "\", \"nbele\": " + value.getNbElement() + ", \"total\": " + value.getTotal() + ", \"mean\": " + value.getMean() + ", \"stddev\": " + value.getStandardDeviation()+", \"values\": ["+ value.dump() + "] }";
                    System.out.println(jsonResult);
                    return new KeyValue<>(key.key(), jsonResult);
                });


        // convert stream to ktable so at this stage this could be use later on to do a join on a stream and compare some values
        KTable<String, String> convertedTable = convertedStream.toTable();


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        this.print(dateFormat.format(new Date()) + ">Starting ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //--------------------------------------------------

}
