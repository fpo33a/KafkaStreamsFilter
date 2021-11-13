

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
    private final String applicationId = "test";
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString()) ; // this.applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SensorRecordDateTimeExtractor.class.getName()); // to extract business date from record
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // generate some data into input topic
        this.print(dateFormat.format(new Date()) + ">Starting data generator thread ...");
/*
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
                return sensorAggregator.getData().getBytes();
            }
        } ;
        Deserializer<SensorAggregator> deserializer = new Deserializer<SensorAggregator>() {
            @Override
            public SensorAggregator deserialize(String s, byte[] bytes) {
                SensorAggregator sensorAggregator = new SensorAggregator();
                sensorAggregator.setData(new String(bytes));
                // wrong here: it should reparse all to get correct total & nb ele
                // wrong - should not concat string but update existing one
                return  (sensorAggregator);
            }
        } ;

        KTable<Windowed<String>, SensorAggregator> testTable = builder.stream(this.topicName, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                //Add a 10 second window and slide the hopping time window in 5 second steps
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10).toMillis()).advanceBy(Duration.ofSeconds(5).toMillis()))
                .aggregate(SensorAggregator::new, // new SensorInitiator (),
                        (aggKey, newValue, aggValue) -> {
                            //System.out.println("aggKey" + aggKey + " newValue " + newValue + " aggValue " + aggValue.getData());
                            aggValue.setData(newValue);
                            //aggValue = aggValue + " - " + newValue;
                            return aggValue;
                        },
                        //Materialized.with(Serdes.String(),Serdes.String()
                        Materialized.with(Serdes.String(), Serdes.serdeFrom(serializer, deserializer))
                );

        KStream<Windowed<String>, SensorAggregator> testStream = testTable.toStream()
                .map((Windowed<String> key, SensorAggregator value) -> {
                    System.out.println(">   map : " + key + " : TOTAL : " + value.getTotal() + " - " + value.getData() );
                    return new KeyValue<>(key, value);
                });

/*

        KTable<Windowed<String>, Long> testTable = builder.stream(this.topicName, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                //Add a 10 second window and slide the hopping time window in 5 second steps
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10).toMillis()).advanceBy(Duration.ofSeconds(5).toMillis()))
                .count();
        KStream<String, Long> testStream = testTable.toStream()
                .map((Windowed<String> key, Long value) -> {
                            System.out.println(">   map : " + key + " - " + value);
                            return new KeyValue<>(key.key(), value);
                        });

*/
/*
        TimeWindowedKStream<String, String> windowedTable = builder.stream(this.topicName, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                //Add a 10 second window and slide the hopping time window in 5 second steps
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10).toMillis()).advanceBy(Duration.ofSeconds(5).toMillis()) )


        KTable<Windowed<String>, SensorAggregator> testTable = windowedTable
                .aggregate(SensorAggregator::new, (key, value, aggregator) -> {
                    System.out.println(" key : "+key + " value : "+value);
                    aggregator.setNbElement(aggregator.getNbElement() + 1);
                    int sensorValue = Integer.parseInt(this.getSensorValue("SensorValue", value));        // should be safer !
                    aggregator.setTotal(aggregator.getTotal() + sensorValue);
                    aggregator.setSensor((String) key);
                    aggregator.dump();
                    return aggregator;
                }, Materialized.with(Serdes.String(),Serdes.serdeFrom(serializer,deserializer)));

        KStream<Windowed<String>, String> testStream = testTable.toStream()
                .map((Windowed<String> key, SensorAggregator value) -> {
                    System.out.println(">   map : " + key + " - " + value );
                    return new KeyValue<>(key, value.toJson());
                });
*/

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        this.print(dateFormat.format(new Date()) + ">Starting ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    //--------------------------------------------------

    // ok this is not the right way to do a json field content extraction but this is fast
    private String getSensorValue(String key, String value) {
        String field = "\"" + key + "\": ";
        int pos = value.indexOf(field);
        int start = pos + field.length();
        int end = value.indexOf("}", start);
        String result = value.substring(start, end);
        return result;
    }

    //--------------------------------------------------

}

