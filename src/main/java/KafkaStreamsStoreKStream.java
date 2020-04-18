/*

Creating a (in memory) state store on top of a KStream
To always get latest value when fetching data per key we add a processor that update the store
the "MyReader" Thread is a dummy consumer that pull data for key "bb" every second to illustrate state

*/
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.time.LocalDateTime;
import java.util.Properties;

public class KafkaStreamsStoreKStream {

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
         streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder  builder = new StreamsBuilder();

        final String storeName = "fpStore";

        final StoreBuilder<KeyValueStore<String, String>> fpStore =
                Stores
                        .keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(storeName),
                                Serdes.String(),
                                Serdes.String()
                        )
                        .withCachingEnabled();
        builder.addStateStore(fpStore);
        KStream<String, String> data = builder.stream("storeInput");
        data.process(() -> new MyProcessor(storeName), storeName);

        System.out.println("Start running store");
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        MyReader reader = new MyReader();
        reader.init(streams,storeName);
        Thread thread = new Thread (reader);
        thread.start();

    }

    //--------------------------------------------------------------------

    private static ReadOnlyKeyValueStore<String, String> waitUntilStoreIsQueryable(final String storeName, final KafkaStreams streams) {
        System.out.println("get store ...");
        while (true) {
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

    static class MyReader implements Runnable {

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
            ReadOnlyKeyValueStore<String, String> view = KafkaStreamsStoreKStream.waitUntilStoreIsQueryable(storeName,streams);

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

    private static class MyProcessor implements Processor<String, String>
    {
        private ProcessorContext context = null;
        private KeyValueStore<String, String> stateStore = null;
        private String stateStoreName = null;

        public MyProcessor(String store)
        {
            this.stateStoreName = store;
        }

        @Override
        public void init(ProcessorContext processorContext) {
            this.context = processorContext;
            this.stateStore = (KeyValueStore<String, String>) this.context.getStateStore(stateStoreName);

        }

        @Override
        public void process(String key, String value) {
            System.out.println ("process: Updating store for key "+key+" with value "+value);
            stateStore.put(key, value);
        }

        @Override
        public void close() {

        }
    }

    //--------------------------------------------------------------------

}
