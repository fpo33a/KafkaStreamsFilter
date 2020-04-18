/*

Creating a (in memory) state store on top of a KTable -> always get latest value when fetching it
the "MyReader" Thread is a dummy consumer that pull data for key "bb" every second to illustrate state

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic storeInput

>aa:000
>bb:222
>bb:333
>bb:444
>aa:000
>ee:654
>bb:012
>

C:\jdk1.8\bin\java.exe -javaagent:C:\frank\intellij\lib\idea_rt.jar=63435:C:\frank\intellij\bin -Dfile.encoding=UTF-8 -classpath C:\jdk1.8\jre\lib\charsets.jar;C:\jdk1.8\jre\lib\deploy.jar;C:\jdk1.8\jre\lib\ext\access-bridge-64.jar;C:\jdk1.8\jre\lib\ext\cldrdata.jar;C:\jdk1.8\jre\lib\ext\dnsns.jar;C:\jdk1.8\jre\lib\ext\jaccess.jar;C:\jdk1.8\jre\lib\ext\jfxrt.jar;C:\jdk1.8\jre\lib\ext\localedata.jar;C:\jdk1.8\jre\lib\ext\nashorn.jar;C:\jdk1.8\jre\lib\ext\sunec.jar;C:\jdk1.8\jre\lib\ext\sunjce_provider.jar;C:\jdk1.8\jre\lib\ext\sunmscapi.jar;C:\jdk1.8\jre\lib\ext\sunpkcs11.jar;C:\jdk1.8\jre\lib\ext\zipfs.jar;C:\jdk1.8\jre\lib\javaws.jar;C:\jdk1.8\jre\lib\jce.jar;C:\jdk1.8\jre\lib\jfr.jar;C:\jdk1.8\jre\lib\jfxswt.jar;C:\jdk1.8\jre\lib\jsse.jar;C:\jdk1.8\jre\lib\management-agent.jar;C:\jdk1.8\jre\lib\plugin.jar;C:\jdk1.8\jre\lib\resources.jar;C:\jdk1.8\jre\lib\rt.jar;C:\frank\KafkaStreamsFilter\target\classes;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-streams\2.4.1\kafka-streams-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-json\2.4.1\connect-json-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-api\2.4.1\connect-api-2.4.1.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.10.0\jackson-databind-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.10.0\jackson-annotations-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.10.0\jackson-core-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\datatype\jackson-datatype-jdk8\2.10.0\jackson-datatype-jdk8-2.10.0.jar;C:\Users\frank\.m2\repository\org\slf4j\slf4j-api\1.7.28\slf4j-api-1.7.28.jar;C:\Users\frank\.m2\repository\org\rocksdb\rocksdbjni\5.18.3\rocksdbjni-5.18.3.jar;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-clients\2.4.1\kafka-clients-2.4.1.jar;C:\Users\frank\.m2\repository\com\github\luben\zstd-jni\1.4.3-1\zstd-jni-1.4.3-1.jar;C:\Users\frank\.m2\repository\org\lz4\lz4-java\1.6.0\lz4-java-1.6.0.jar;C:\Users\frank\.m2\repository\org\xerial\snappy\snappy-java\1.1.7.3\snappy-java-1.1.7.3.jar KafkaStreamsStoreKTable
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Start running store
fpStore
get store ...
still wait for store ...Cannot get state store fpStore because the stream thread is STARTING, not RUNNING
still wait for store ...Cannot get state store fpStore because the stream thread is STARTING, not RUNNING
still wait for store ...Cannot get state store fpStore because the stream thread is PARTITIONS_ASSIGNED, not RUNNING
key = bb, value = 333
key = aa, value = 000
key = bb, value = 444
2020-04-18T10:06:47.577 : value for key bb = 444
2020-04-18T10:06:49.591 : value for key bb = 444
2020-04-18T10:06:51.591 : value for key bb = 444
2020-04-18T10:06:53.592 : value for key bb = 444
2020-04-18T10:06:55.593 : value for key bb = 444
2020-04-18T10:06:57.599 : value for key bb = 444
2020-04-18T10:06:59.599 : value for key bb = 444
2020-04-18T10:07:01.600 : value for key bb = 444
2020-04-18T10:07:03.601 : value for key bb = 444
key = ee, value = 654
2020-04-18T10:07:05.601 : value for key bb = 444
2020-04-18T10:07:07.601 : value for key bb = 444
2020-04-18T10:07:09.602 : value for key bb = 444
key = bb, value = 012
2020-04-18T10:07:11.602 : value for key bb = 012
2020-04-18T10:07:13.602 : value for key bb = 012
2020-04-18T10:07:15.602 : value for key bb = 012
2020-04-18T10:07:17.602 : value for key bb = 012
2020-04-18T10:07:19.603 : value for key bb = 012

Process finished with exit code -1

 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.LocalDateTime;
import java.util.Properties;

public class KafkaStreamsStoreKTable {

    //---------------------------------------------------------------------------------------------

    public static void main( String[] args ) throws InterruptedException {
        Properties streamsConfig = new Properties();
        // The name must be unique on the Kafka cluster
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-example");
        // Brokers
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9092");
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder  builder = new StreamsBuilder();

        final String storeName = "fpStore";

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        KeyValueStore<Bytes, byte[]> x = storeSupplier.get();

        KTable<String,String> ktable = builder.table( "storeInput",
                Materialized.<String,String>as(storeSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled());
        // the below map purpose is only to track changes detected
        KStream<String,String> data = ktable.toStream().map( (k,v) -> { System.out.println("key = "+k + ", value = "+v); return KeyValue.pair(k,v);});

        System.out.println("Start running store");
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.cleanUp();
        streams.start();
        System.out.println(ktable.queryableStoreName());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        MyReader reader = new MyReader();
        reader.init(streams,storeName);
        Thread thread = new Thread (reader);
        thread.start();
    }

    //--------------------------------------------------------------------

    private static ReadOnlyKeyValueStore<String, String> waitUntilStoreIsQueryable(final String storeName, final KafkaStreams streams) {
        System.out.println("get store ...");
        while (true)
        {
            try {
                return streams.store(storeName, QueryableStoreTypes.<String, String>keyValueStore());
            } catch (InvalidStateStoreException ignored) {
                // Journalisation
                System.out.println("still wait for store ..."+ignored.getMessage());
                // store not yet ready for querying
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //--------------------------------------------------------------------

    static class MyReader implements Runnable
    {

        final String key = "bb";
        KafkaStreams streams = null;
        String storeName = null;

        public void init (KafkaStreams s, String store)
        {
            this.storeName = store;
            this.streams = s;
        }

        @Override
        public void run()
        {
            ReadOnlyKeyValueStore<String, String> view = KafkaStreamsStoreKTable.waitUntilStoreIsQueryable(storeName,streams);

            while (true)
            {
                final String value = view.get(key);
                if (value == null) System.out.println (LocalDateTime.now()+" : No value found for key : "+key);
                else System.out.println (LocalDateTime.now()+" : value for key "+key+" = "+value);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //--------------------------------------------------------------------

}
