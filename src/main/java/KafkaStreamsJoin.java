/*
ested enrichment use case
data : key is not set in kafka - we need to extract it from the value - 5000 <> keys for test with a data set of 100.000 records ( 20 per key )
refdata : key is set - 5000 <> keys for test
Test condition:

1/ machine used:  windows 8 on home laptop - intel celeron cpu 1005M - 1.9 Ghz - 8GB Ram - spinning disk
2/ Apache Kafka 2.4.1
3/ Apache  zookeeper 3.6.0

4/ Kafka setup:

C:\frank\apache-kafka-2.4.1\bin\windows>dir c:\frank\apa*
 Le volume dans le lecteur C s'appelle TI31128200B
 Le numéro de série du volume est F063-B02B

 Répertoire de c:\frank

04-04-20  19:25    <DIR>          apache-kafka-2.4.1
04-04-20  19:22    <DIR>          apache-zookeeper-3.6.0

cd C:\frank\apache-zookeeper-3.6.0\bin
zkServer.cmd

cd C:\frank\apache-kafka-2.4.1\bin\windows
kafka-server-start.bat ..\..\config\server.properties

kafka-topics.bat --zookeeper localhost:2181 --delete --topic ktest
kafka-topics.bat --zookeeper localhost:2181 --delete --topic kref
kafka-topics.bat --zookeeper localhost:2181 --delete --topic kresult

kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic ktest
kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic kref
kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 4 --topic kresult

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-topics.bat --zookeeper localhost:2181 --describe
Topic: __consumer_offsets       PartitionCount: 50      ReplicationFactor: 1    Configs: compression.type=producer,cleanup.policy=compact,segment.bytes=104857600
        Topic: __consumer_offsets       Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 1    Leader: 0       Replicas: 0     Isr: 0
[...]
        Topic: __consumer_offsets       Partition: 47   Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 48   Leader: 0       Replicas: 0     Isr: 0
        Topic: __consumer_offsets       Partition: 49   Leader: 0       Replicas: 0     Isr: 0
Topic: kref     PartitionCount: 4       ReplicationFactor: 1    Configs:
        Topic: kref     Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: kref     Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: kref     Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: kref     Partition: 3    Leader: 0       Replicas: 0     Isr: 0
Topic: kresult  PartitionCount: 4       ReplicationFactor: 1    Configs:
        Topic: kresult  Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: kresult  Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: kresult  Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: kresult  Partition: 3    Leader: 0       Replicas: 0     Isr: 0
Topic: ktest    PartitionCount: 4       ReplicationFactor: 1    Configs:
        Topic: ktest    Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: ktest    Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: ktest    Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: ktest    Partition: 3    Leader: 0       Replicas: 0     Isr: 0
Topic: testjoin1-KSTREAM-KEY-SELECT-0000000001-repartition      PartitionCount: 4       ReplicationFactor: 1    Configs: cleanup.policy=delete,segment.bytes=52428800,retention.ms=-1
        Topic: testjoin1-KSTREAM-KEY-SELECT-0000000001-repartition      Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-KSTREAM-KEY-SELECT-0000000001-repartition      Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-KSTREAM-KEY-SELECT-0000000001-repartition      Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-KSTREAM-KEY-SELECT-0000000001-repartition      Partition: 3    Leader: 0       Replicas: 0     Isr: 0
Topic: testjoin1-kref-STATE-STORE-0000000002-changelog  PartitionCount: 4       ReplicationFactor: 1    Configs: cleanup.policy=compact
        Topic: testjoin1-kref-STATE-STORE-0000000002-changelog  Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-kref-STATE-STORE-0000000002-changelog  Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-kref-STATE-STORE-0000000002-changelog  Partition: 2    Leader: 0       Replicas: 0     Isr: 0
        Topic: testjoin1-kref-STATE-STORE-0000000002-changelog  Partition: 3    Leader: 0       Replicas: 0     Isr: 0

kafka-console-consumer.bat -bootstrap-server localhost:9092 --from-beginning --property print.key=true --property print.timestamp=true --topic ktresult

Line 0      : CreateTime:1589645542024	Key9	{ "key": "Key9","data": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9", "key": "Key9","reference_data": "reference_9"}}
Line 1      : CreateTime:1589645542024	Key11	{ "key": "Key11","data": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA11", "key": "Key11","reference_data": "reference_11"}}
[...]
line 100000 : CreateTime:1589645555720	Key4998	{ "key": "Key4998","data": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA99998", "key": "Key4998","reference_data": "reference_4998"}}


5/ results ( see attachments )
one instance of kstream application, running on same host as 1 node kafka cluster
kafka stream using only 1 thread
kafka topics using 4 partitions
100.000 data created in ktest topic
data record size :  +- 1050 bytes
5.000 ref data created in kref topic
refdata record size : +- 100 bytes
launch ( from intellij ) kstream join process
=> produce 100.000 joined records in kresult topic
Saturday 16 May 2020 16:12:22.024 GMT ( first "joined" message )
Saturday 16 May 2020 16:12:35.720 GMT ( last "joined" message )

100.000 data with 5.000 ref data  in 14.7 sec -> 6802 msg enriched per sec

 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
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
    private int nbRefData = 5000;
    private int nbRec = 100000;
    private int recordLengh = 1024;
    private String action = "enrich";
    private boolean verbose = true;

    //-------------------------------------------------------------------


    public static void main(String[] args) {
        KafkaStreamsJoin test = new KafkaStreamsJoin();
        test.skipOutdated();
        test.getArgs(args);
        if (test.action.compareToIgnoreCase("enrich") == 0) test.enrichData();
        else if (test.action.compareToIgnoreCase("refdata") == 0) test.generateRefData();
        else if (test.action.compareToIgnoreCase("data") == 0) test.generateData();
        else if (test.action.compareToIgnoreCase("outdated") == 0) test.skipOutdated();
        return;
    }

    //-------------------------------------------------------------------

    private void getArgs (String[] args)
    {
        for (int i = 0; i < args.length-1; i++)             // length-1 as last arg should be a value not an arg name
        {
            if (args[i].compareToIgnoreCase("-nbRec") == 0) this.nbRec = Integer.parseInt(args[i+1]);
            else if (args[i].compareToIgnoreCase("-nbRefData") == 0) this.nbRefData = Integer.parseInt(args[i+1]);
            else if (args[i].compareToIgnoreCase("-recordLength") == 0) this.recordLengh = Integer.parseInt(args[i+1]);
            else if (args[i].compareToIgnoreCase("-action") == 0) this.action = args[i+1];
            else if (args[i].compareToIgnoreCase("-v") == 0)
            {
                if (args[i+1].compareToIgnoreCase("true") == 0) this.verbose = true;
                else this.verbose = false;
            }
        }
        this.print ("Parameters:");
        this.print (" nbRec       : "+this.nbRec);
        this.print (" nbRefData   : "+this.nbRefData);
        this.print (" recordLengh : "+this.recordLengh);
        this.print (" action      : "+this.action);
        this.print ("");

    }

    //-------------------------------------------------------------------

    private void print (String data)
    {
        if (this.verbose) System.out.println (data);
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

        this.print (dateFormat.format(new Date())+">Ref data generation starting");
        StringBuilder dataBuilder = new StringBuilder("\"data\": \"");
        for (int i = 0; i < this.recordLengh; i++) dataBuilder.append ("A");

        for (int i = 0; i  < this.nbRefData; i++)
        {
            try {
                String key = "Key"+i;
                String record = "{ \"key\": \""+key+"\","+"\"reference_data\": \"reference_"+i+"\"}";

                if (i%100 == 0) this.print (dateFormat.format(new Date())+">Produced "+i);

                final ProducerRecord<String, String> pr = new ProducerRecord<>("kref", key, record);
                producer.send(pr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.print (dateFormat.format(new Date())+">Ref data generation done");
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

        this.print (dateFormat.format(new Date())+">Data generation starting");
        StringBuilder dataBuilder = new StringBuilder("\"data\": \"");
        for (int i = 0; i < this.recordLengh; i++) dataBuilder.append ("A");

        for (int i = 0; i  < this.nbRec; i++)
        {
            try {
                String key = "Key"+(i%this.nbRefData);
                String record = "{ \"key\": \""+key+"\","+dataBuilder.toString()+i+"\"}";

                if (i%100 == 0) System.out.println(dateFormat.format(new Date())+">Produced "+i);

                final ProducerRecord<String, String> pr = new ProducerRecord<>("ktest", key, record);
                producer.send(pr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.print (dateFormat.format(new Date())+">Ref data generation done");
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
                return getValueByKey ("key",v);
        })   ;

        KTable<String, String> refStream = builder.table ("kref");

        KStream<String, String> resultStream = dataStream.leftJoin(refStream,
                (testValue, refValue) -> {
                    return MergeValues(testValue,refValue);
                });

        // display join result
        if (this.verbose) {
            resultStream
                    .foreach((k, v) -> {
                        System.out.println(dateFormat.format(new Date()) + "> Key: [ " + k + " ]  Value: [ " + v + " ]");
                    });
        }
        // send result into result topic
        resultStream.to("kresult", Produced.with (Serdes.String(), Serdes.String()) );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        this.print (dateFormat.format(new Date()) + ">Starting ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //--------------------------------------------------

    // ok this is not the right way to do a json field content extraction but this is fast
    private String getValueByKey ( String key, String value)
    {
        String field = "\""+key+"\": \"";
//        int pos = value.indexOf("\"key\": \"");
//        int start = pos + "\"key\": \"".length();
        int pos = value.indexOf(field);
        int start = pos + field.length();
        int end = value.indexOf("\"",start);
        String result = value.substring(start,end);
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

    //-------------------------------------------------------------------

    /*

    skipOutdatd function: purpose is to skip messages that are entering out of band, outdated based on business date
    data ( field "time" in json record ).

    1. create some topics

    cd C:\frank\apache-zookeeper-3.6.0\bin
    zkServer.cmd

    cd C:\frank\apache-kafka-2.4.1\bin\windows
    kafka-server-start.bat ..\..\config\server.properties

    kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic input
    kafka-topics.bat  --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic latest

    2. produce some records
    kafka-console-producer.bat --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic input
    >cc:{"time": "20200101 00:00:00", "data": "cc0" }
    >cc:{"time": "20200101 00:01:00", "data": "cc1" }
    >cc:{"time": "20200101 00:05:00", "data": "cc5" }
    >cc:{"time": "20200101 00:02:00", "data": "cc2" }
    >cc:{"time": "20200101 00:07:00", "data": "cc7" }
    >cc:{"time": "20200101 00:04:00", "data": "cc4" }
    >cc:{"time": "20200101 00:09:00", "data": "cc9" }

    3. check result in output
    C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --property print.key=true
    cc      {"time": "20200101 00:00:00", "data": "cc0" }
    cc      {"time": "20200101 00:01:00", "data": "cc1" }
    cc      {"time": "20200101 00:05:00", "data": "cc5" }
    cc      {"time": "20200101 00:07:00", "data": "cc7" }
    cc      {"time": "20200101 00:09:00", "data": "cc9" }

     As we can see the records at 00:02:00 has been skip as last at that moment was 00:05:00
     As we can see the records at 00:04:00 has been skip as last at that moment was 00:07:00
     */

    private void skipOutdated()
    {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,this.nbStreamsThread);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> intput = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> klatest = builder.table ("latest", Consumed.with(Serdes.String(), Serdes.String()))
                // we rename "time" in "last" in ktable ( should not be a replace but a true json field rename, just short cut for demo)
                // purpose: ktable record will be joined with input table - if we don't rename in joined result we will have 2 fields "time"
                // which is not pretty to do extraction & comparison
                .mapValues( value -> ( value.replace("time","last")) );

        KStream<String, String> filterStream = intput.leftJoin(klatest,
                (left, right) -> {
                    // we merge the two json records if join returns result, else just return input record
                    if (right != null) return left+right;
                    return left;
                })
                .filter( (key, value) ->  keepLatest(key,value) )
                .mapValues( value -> (  value = splitJson(value) ) );

        // send result into result topic
        filterStream.to("latest", Produced.with(Serdes.String(),Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        this.print (dateFormat.format(new Date()) + ">Starting ...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //---------------------------------------------------------------------------------------------
    // check if "time" is > than "last" - here just use string compare for ease of use ( maybe better to use date comparison )

    private  boolean keepLatest (String key, String value)
    {
        String time = getValueByKey("time",value);
        String last = getValueByKey("last",value);
        System.out.println("keepLatest - "+time+" - "+last);
        if (last.compareToIgnoreCase(time) > 0) return false;
        return true;
    }

    //--------------------------------------------------

    // ok this is not the best way to do a "double" json split but this is fast & works
    private String splitJson (String value)
    {
        String result = "";
        if (value == null) return null;
        int pos = value.indexOf("}{");
        if (pos >= 0) result = value.substring(0,pos+1);
        else result = value;
        System.out.println("splitJson "+result);
        return result;
    }

    //--------------------------------------------------

}

