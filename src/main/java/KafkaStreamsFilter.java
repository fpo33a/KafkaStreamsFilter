/*
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
 */
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamsFilter
{
    public static void main( String[] args ) {
        Properties streamsConfig = new Properties();
        // The name must be unique on the Kafka cluster
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "fp-example");
        // Brokers
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder  builder = new StreamsBuilder();
        KStream<String, String> dataStream = builder.stream("test", Consumed.with(Serdes.String(), Serdes.String()))
                .filter( (key, value) ->  {
                    if (value.indexOf("hello") >= 0)
                    {
                        System.out.println ("Accept "+value);
                        return true;
                    }
                    System.out.println ("Reject "+value);
                    return false;
                } );
        dataStream.to("filter", Produced.with(Serdes.String(),Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}