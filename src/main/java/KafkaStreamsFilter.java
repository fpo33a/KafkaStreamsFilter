/*

-------------------------------------------------------------------------------
1/ FILTER TEST
-------------------------------------------------------------------------------
C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic test
>hello toto
>hello tutu
>hey titi
>hi tata
>hello tete
>Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
hello toto
hello tutu
hey titi
hi tata
hello tete
Processed a total of 5 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic filter
Processed a total of 0 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic filter --from-beginning
hello toto
hello tutu
hello tete
Processed a total of 3 messages
Terminer le programme de commandes (O/N) ? o

C:\jdk1.8\bin\java.exe -javaagent:C:\frank\intellij\lib\idea_rt.jar=62460:C:\frank\intellij\bin -Dfile.encoding=UTF-8 -classpath C:\jdk1.8\jre\lib\charsets.jar;C:\jdk1.8\jre\lib\deploy.jar;C:\jdk1.8\jre\lib\ext\access-bridge-64.jar;C:\jdk1.8\jre\lib\ext\cldrdata.jar;C:\jdk1.8\jre\lib\ext\dnsns.jar;C:\jdk1.8\jre\lib\ext\jaccess.jar;C:\jdk1.8\jre\lib\ext\jfxrt.jar;C:\jdk1.8\jre\lib\ext\localedata.jar;C:\jdk1.8\jre\lib\ext\nashorn.jar;C:\jdk1.8\jre\lib\ext\sunec.jar;C:\jdk1.8\jre\lib\ext\sunjce_provider.jar;C:\jdk1.8\jre\lib\ext\sunmscapi.jar;C:\jdk1.8\jre\lib\ext\sunpkcs11.jar;C:\jdk1.8\jre\lib\ext\zipfs.jar;C:\jdk1.8\jre\lib\javaws.jar;C:\jdk1.8\jre\lib\jce.jar;C:\jdk1.8\jre\lib\jfr.jar;C:\jdk1.8\jre\lib\jfxswt.jar;C:\jdk1.8\jre\lib\jsse.jar;C:\jdk1.8\jre\lib\management-agent.jar;C:\jdk1.8\jre\lib\plugin.jar;C:\jdk1.8\jre\lib\resources.jar;C:\jdk1.8\jre\lib\rt.jar;C:\frank\KafkaStreamsFilter\target\classes;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-streams\2.4.1\kafka-streams-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-json\2.4.1\connect-json-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-api\2.4.1\connect-api-2.4.1.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.10.0\jackson-databind-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.10.0\jackson-annotations-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.10.0\jackson-core-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\datatype\jackson-datatype-jdk8\2.10.0\jackson-datatype-jdk8-2.10.0.jar;C:\Users\frank\.m2\repository\org\slf4j\slf4j-api\1.7.28\slf4j-api-1.7.28.jar;C:\Users\frank\.m2\repository\org\rocksdb\rocksdbjni\5.18.3\rocksdbjni-5.18.3.jar;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-clients\2.4.1\kafka-clients-2.4.1.jar;C:\Users\frank\.m2\repository\com\github\luben\zstd-jni\1.4.3-1\zstd-jni-1.4.3-1.jar;C:\Users\frank\.m2\repository\org\lz4\lz4-java\1.6.0\lz4-java-1.6.0.jar;C:\Users\frank\.m2\repository\org\xerial\snappy\snappy-java\1.1.7.3\snappy-java-1.1.7.3.jar KafkaStreamsFilter
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Accept hello toto
Accept hello tutu
Reject hey titi
Reject hi tata
Accept hello tete
Reject salut frank
Reject bonjour marcel
Accept hello vero

Process finished with exit code -1

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic filter --from-beginning
hello toto
hello tutu
hello tete
hello vero

-------------------------------------------------------------------------------
2/ BRANCH TEST
-------------------------------------------------------------------------------

Reuse same data as for testFilter, just use another consumer group to restart from beginning

C:\jdk1.8\bin\java.exe -javaagent:C:\frank\intellij\lib\idea_rt.jar=60789:C:\frank\intellij\bin -Dfile.encoding=UTF-8 -classpath C:\jdk1.8\jre\lib\charsets.jar;C:\jdk1.8\jre\lib\deploy.jar;C:\jdk1.8\jre\lib\ext\access-bridge-64.jar;C:\jdk1.8\jre\lib\ext\cldrdata.jar;C:\jdk1.8\jre\lib\ext\dnsns.jar;C:\jdk1.8\jre\lib\ext\jaccess.jar;C:\jdk1.8\jre\lib\ext\jfxrt.jar;C:\jdk1.8\jre\lib\ext\localedata.jar;C:\jdk1.8\jre\lib\ext\nashorn.jar;C:\jdk1.8\jre\lib\ext\sunec.jar;C:\jdk1.8\jre\lib\ext\sunjce_provider.jar;C:\jdk1.8\jre\lib\ext\sunmscapi.jar;C:\jdk1.8\jre\lib\ext\sunpkcs11.jar;C:\jdk1.8\jre\lib\ext\zipfs.jar;C:\jdk1.8\jre\lib\javaws.jar;C:\jdk1.8\jre\lib\jce.jar;C:\jdk1.8\jre\lib\jfr.jar;C:\jdk1.8\jre\lib\jfxswt.jar;C:\jdk1.8\jre\lib\jsse.jar;C:\jdk1.8\jre\lib\management-agent.jar;C:\jdk1.8\jre\lib\plugin.jar;C:\jdk1.8\jre\lib\resources.jar;C:\jdk1.8\jre\lib\rt.jar;C:\frank\KafkaStreamsFilter\target\classes;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-streams\2.4.1\kafka-streams-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-json\2.4.1\connect-json-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-api\2.4.1\connect-api-2.4.1.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.10.0\jackson-databind-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.10.0\jackson-annotations-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.10.0\jackson-core-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\datatype\jackson-datatype-jdk8\2.10.0\jackson-datatype-jdk8-2.10.0.jar;C:\Users\frank\.m2\repository\org\slf4j\slf4j-api\1.7.28\slf4j-api-1.7.28.jar;C:\Users\frank\.m2\repository\org\rocksdb\rocksdbjni\5.18.3\rocksdbjni-5.18.3.jar;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-clients\2.4.1\kafka-clients-2.4.1.jar;C:\Users\frank\.m2\repository\com\github\luben\zstd-jni\1.4.3-1\zstd-jni-1.4.3-1.jar;C:\Users\frank\.m2\repository\org\lz4\lz4-java\1.6.0\lz4-java-1.6.0.jar;C:\Users\frank\.m2\repository\org\xerial\snappy\snappy-java\1.1.7.3\snappy-java-1.1.7.3.jar KafkaStreamsFilter
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Accept hello toto
Accept hello tutu
Reject hey titi
Reject hi tata
Accept hello tete
Reject salut frank
Reject bonjour marcel
Accept hello vero

Process finished with exit code -1

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092  --from-beginning --topic okfilter
hello toto
hello tutu
hello tete
hello vero
Processed a total of 4 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092  --from-beginning --topic kofilter
hey titi
hi tata
salut frank
bonjour marcel
Processed a total of 4 messages
Terminer le programme de commandes (O/N) ? o

-------------------------------------------------------------------------------
3/ BRANCH DYNAMIC TEST
-------------------------------------------------------------------------------

Reuse same data as for testFilter & testBranch, just use another consumer group to restart from beginning

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic okfilter
hello toto
hello tutu
hello tete
hello vero
hello toto
hello tutu
hello tete
hello vero
hello toto
hello tutu
hello tete
hello vero
Processed a total of 12 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic okfilter-frank
Processed a total of 0 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-producer.bat --broker-list localhost:9092 --topic test
>hello frank
>hello nicole
>frank is for hello
>Terminer le programme de commandes (O/N) ? o

C:\jdk1.8\bin\java.exe -javaagent:C:\frank\intellij\lib\idea_rt.jar=52417:C:\frank\intellij\bin -Dfile.encoding=UTF-8 -classpath C:\jdk1.8\jre\lib\charsets.jar;C:\jdk1.8\jre\lib\deploy.jar;C:\jdk1.8\jre\lib\ext\access-bridge-64.jar;C:\jdk1.8\jre\lib\ext\cldrdata.jar;C:\jdk1.8\jre\lib\ext\dnsns.jar;C:\jdk1.8\jre\lib\ext\jaccess.jar;C:\jdk1.8\jre\lib\ext\jfxrt.jar;C:\jdk1.8\jre\lib\ext\localedata.jar;C:\jdk1.8\jre\lib\ext\nashorn.jar;C:\jdk1.8\jre\lib\ext\sunec.jar;C:\jdk1.8\jre\lib\ext\sunjce_provider.jar;C:\jdk1.8\jre\lib\ext\sunmscapi.jar;C:\jdk1.8\jre\lib\ext\sunpkcs11.jar;C:\jdk1.8\jre\lib\ext\zipfs.jar;C:\jdk1.8\jre\lib\javaws.jar;C:\jdk1.8\jre\lib\jce.jar;C:\jdk1.8\jre\lib\jfr.jar;C:\jdk1.8\jre\lib\jfxswt.jar;C:\jdk1.8\jre\lib\jsse.jar;C:\jdk1.8\jre\lib\management-agent.jar;C:\jdk1.8\jre\lib\plugin.jar;C:\jdk1.8\jre\lib\resources.jar;C:\jdk1.8\jre\lib\rt.jar;C:\frank\KafkaStreamsFilter\target\classes;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-streams\2.4.1\kafka-streams-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-json\2.4.1\connect-json-2.4.1.jar;C:\Users\frank\.m2\repository\org\apache\kafka\connect-api\2.4.1\connect-api-2.4.1.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.10.0\jackson-databind-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.10.0\jackson-annotations-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.10.0\jackson-core-2.10.0.jar;C:\Users\frank\.m2\repository\com\fasterxml\jackson\datatype\jackson-datatype-jdk8\2.10.0\jackson-datatype-jdk8-2.10.0.jar;C:\Users\frank\.m2\repository\org\slf4j\slf4j-api\1.7.28\slf4j-api-1.7.28.jar;C:\Users\frank\.m2\repository\org\rocksdb\rocksdbjni\5.18.3\rocksdbjni-5.18.3.jar;C:\Users\frank\.m2\repository\org\apache\kafka\kafka-clients\2.4.1\kafka-clients-2.4.1.jar;C:\Users\frank\.m2\repository\com\github\luben\zstd-jni\1.4.3-1\zstd-jni-1.4.3-1.jar;C:\Users\frank\.m2\repository\org\lz4\lz4-java\1.6.0\lz4-java-1.6.0.jar;C:\Users\frank\.m2\repository\org\xerial\snappy\snappy-java\1.1.7.3\snappy-java-1.1.7.3.jar KafkaStreamsFilter
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Accept hello toto
-->okfilter : hello toto
Accept hello tutu
-->okfilter : hello tutu
Reject hey titi
Reject hi tata
Accept hello tete
-->okfilter : hello tete
Reject salut frank
Reject bonjour marcel
Accept hello vero
-->okfilter : hello vero
Accept hello frank
-->okfilter-frank : hello frank
Accept hello nicole
-->okfilter : hello nicole
Accept frank is for hello
-->okfilter-frank : frank is for hello

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic okfilter-frank
hello frank
frank is for hello
Processed a total of 2 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic okfilter
hello toto
hello tutu
hello tete
hello vero
hello toto
hello tutu
hello tete
hello vero
hello toto
hello tutu
hello tete
hello vero
hello nicole
Processed a total of 13 messages
Terminer le programme de commandes (O/N) ? o

C:\frank\apache-kafka-2.4.1\bin\windows>


 */
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Properties;

public class KafkaStreamsFilter
{
    //---------------------------------------------------------------------------------------------

    public static void main( String[] args ) {
        Properties streamsConfig = new Properties();
        // The name must be unique on the Kafka cluster
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "branchdynamic-example");
        // Brokers
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

//        StreamsBuilder  builder = KafkaStreamsFilter.testFilter(streamsConfig,"test","filter");
//        StreamsBuilder  builder = KafkaStreamsFilter.testBranch(streamsConfig,"test","test","okfilter", "kofilter");
        StreamsBuilder  builder = KafkaStreamsFilter.testBranchDynamic(streamsConfig,"test","okfilter", "kofilter");
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    //---------------------------------------------------------------------------------------------
    // filter data - only keep records matching condition

    public static StreamsBuilder testFilter (Properties streamsConfig,String inputTopic,String outputTopic)
    {
        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String> dataStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .filter( (key, value) ->  KafkaStreamsFilter.testCondition(key,value) );
        dataStream.to(outputTopic, Produced.with(Serdes.String(),Serdes.String()));
        return builder;
    }

    //---------------------------------------------------------------------------------------------
    // filter data - put keep records matching condition in 'ok' topic, push non matching ones in 'ko' topic

    public static StreamsBuilder testBranch (Properties streamsConfig,String inputTopic,String okTopic,String koTopic)
    {
        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String> dataStream [] = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .branch( (key, value) -> KafkaStreamsFilter.testCondition(key,value),
                         (key, value) -> true);
        dataStream[0].to(okTopic, Produced.with(Serdes.String(),Serdes.String()));
        dataStream[1].to(koTopic, Produced.with(Serdes.String(),Serdes.String()));
        return builder;
    }

    //---------------------------------------------------------------------------------------------
    // filter data - put keep records matching condition in 'ok' DYNAMIC topic, push non matching ones in 'ko' topic

    public static StreamsBuilder testBranchDynamic (Properties streamsConfig,String inputTopic,String okTopic,String koTopic)
    {
        StreamsBuilder  builder = new StreamsBuilder();
        TopicSelector topicSelector = new TopicSelector();
        KStream<String, String> dataStream [] = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .branch( (key, value) -> KafkaStreamsFilter.testCondition(key,value),
                        (key, value) -> true);
        dataStream[0].to(topicSelector);            // dynamically set output topic name
        dataStream[1].to(koTopic, Produced.with(Serdes.String(),Serdes.String()));
        return builder;
    }


    //---------------------------------------------------------------------------------------------
    // check presence of word "hello"  as dummy filtering condition

    private static boolean testCondition (String key, String value)
    {
        if (value.indexOf("hello") >= 0)
        {
            System.out.println ("Accept "+value);
            return true;
        }
        System.out.println ("Reject "+value);
        return false;
    }

    //---------------------------------------------------------------------------------------------
    // dynamically select output topic name, based on value content (in this dummy case presence of word  "frank"

    private static class TopicSelector implements TopicNameExtractor<String,String>
    {
        @Override
        public String extract(String key, String value, RecordContext recordContext)
        {
            if (value.indexOf("frank") >= 0)
            {
                System.out.println ("-->okfilter-frank : "+value);
                return "okfilter-frank";
            }
            System.out.println ("-->okfilter : "+value);
            return "okfilter";        }
    }

    //---------------------------------------------------------------------------------------------

}