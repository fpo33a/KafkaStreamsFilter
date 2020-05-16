/*
kafka-topics.bat --zookeeper localhost:2181 --delete --topic ktest
kafka-topics.bat --zookeeper localhost:2181 --delete --topic kref
kafka-topics.bat --zookeeper localhost:2181 --delete --topic kresult

kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic ktest
kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic kref
kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic kresult

kafka-console-producer.bat --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=|" --topic kref
KeyA|"reference-data": "reference A"
KeyB|"reference-data": "reference B"
KeyC|"reference-data": "reference C"
KeyD|"reference-data": "reference D"

kafka-console-producer.bat --broker-list localhost:9092 --topic ktest
{"field1": "data A", "field2": "data AA", "key": "KeyA"}
{"field1": "data B", "field2": "data BB", "key": "KeyB"}
{"field1": "data C", "field2": "data CC", "key": "KeyC"}
{"field1": "data D", "field2": "data DD", "key": "KeyD"}
{"field1": "data E", "field2": "data EE", "key": "KeyE"}


kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --property print.key=true --topic kresult

 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.StringSerializer;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaStreamsJoin {

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private final String bootstrapServer = "localhost:9092";
    private final String applicationId = "testjoin1";
    private final int nbStreamsThread = 1;
    private int nbRec = 10000; // 500 for enrich;
    private int recordLengh = 1024;
    private String action = "enrich";

    //-------------------------------------------------------------------


    public static void main(String[] args) {
        KafkaStreamsJoin test = new KafkaStreamsJoin();
        test.getArgs(args);
        if (test.action.compareToIgnoreCase("enrich") == 0) test.enrichData();
        else if (test.action.compareToIgnoreCase("refdata") == 0) test.generateRefData();
        else if (test.action.compareToIgnoreCase("data") == 0) test.generateData();
        return;
    }

    //-------------------------------------------------------------------

    private void getArgs (String[] args)
    {
        for (int i = 0; i < args.length-1; i++)
        {
            if (args[i].compareToIgnoreCase("-nbRec") == 0) this.nbRec = Integer.parseInt(args[i+1]);
            else if (args[i].compareToIgnoreCase("-recordLength") == 0) this.recordLengh = Integer.parseInt(args[i+1]);
            else if (args[i].compareToIgnoreCase("-action") == 0) this.action = args[i+1];
        }
        System.out.println ("Parameters:");
        System.out.println (" nbRec       : "+this.nbRec);
        System.out.println (" recordLengh : "+this.recordLengh);
        System.out.println (" action      : "+this.action);
        System.out.println ("");

    }

    //-------------------------------------------------------------------

    private void generateRefData ()
    {
        // Initialize producer
        final Properties props_producer = new Properties();
        props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props_producer.put(ProducerConfig.CLIENT_ID_CONFIG, System.getProperty("user.name"));
        props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props_producer.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");

        final Producer<String, String> producer = new KafkaProducer<>(props_producer);

        StringBuilder dataBuilder = new StringBuilder("\"data\": \"");
        for (int i = 0; i < this.recordLengh; i++) dataBuilder.append ("A");

        for (int i = 0; i  < this.nbRec; i++)
        {
            try {
                String key = "Key"+i;
                String record = "{ \"key\": \""+key+"\","+"\"reference_data\": \"reference_"+i+"\"}";

                if (i%100 == 0) System.out.println(dateFormat.format(new Date())+">Produced "+i);

                final ProducerRecord<String, String> pr = new ProducerRecord<>("kref", key, record);
                producer.send(pr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    //-------------------------------------------------------------------

    private void generateData ()
    {
        // Initialize producer
        final Properties props_producer = new Properties();
        props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props_producer.put(ProducerConfig.CLIENT_ID_CONFIG, System.getProperty("user.name"));
        props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props_producer.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");

        final Producer<String, String> producer = new KafkaProducer<>(props_producer);

        StringBuilder dataBuilder = new StringBuilder("\"data\": \"");
        for (int i = 0; i < this.recordLengh; i++) dataBuilder.append ("A");

        for (int i = 0; i  < this.nbRec; i++)
        {
            try {
                String key = "Key"+(i%500);
                String record = "{ \"key\": \""+key+"\","+dataBuilder.toString()+i+"\"}";

                if (i%100 == 0) System.out.println(dateFormat.format(new Date())+">Produced "+i);

                final ProducerRecord<String, String> pr = new ProducerRecord<>("ktest", key, record);
                producer.send(pr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    //-------------------------------------------------------------------

    private void enrichData()
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,this.nbStreamsThread);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> testStream = builder.stream("ktest");
        KStream<String, String> dataStream = testStream.selectKey( (k,v) -> {
                return getKey (k,v);
        })   ;

        KTable<String, String> refStream = builder.table ("kref");

        KStream<String, String> resultStream = dataStream.leftJoin(refStream,
                (testValue, refValue) -> {
                    return MergeValues(testValue,refValue);
                });

        // display join result
        resultStream
                .foreach((k, v) -> {
                    System.out.println(dateFormat.format(new Date())+"> Key: [ " + k + " ]  Value: [ " + v + " ]");
                });
        // send result into result topic
        resultStream.to("kresult", Produced.with (Serdes.String(), Serdes.String()) );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        System.out.println("Starting ..."+new Date());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //--------------------------------------------------
    private String getKey ( String key, String value)
    {
        int pos = value.indexOf("\"key\": \"");
        int start = pos + "\"key\": \"".length();
        int end = value.indexOf("\"",start);
        String result = value.substring(start,end);
        System.out.println("key = "+result);
        return result;
    }

    //--------------------------------------------------

    // ok this is not the right way to do a json concatenation but this is fast
    private String MergeValues (String left, String right)
    {
        //System.out.println("merging "+left+" with "+right);
        if (right == null) return left;
        String result = left.replace ("}", ","+right.substring(1) + "}");
        return result;
    }

    //--------------------------------------------------

}

