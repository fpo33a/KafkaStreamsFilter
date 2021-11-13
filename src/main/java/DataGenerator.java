import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class DataGenerator implements Runnable {

    private String bootstrapServer = "";
    private String topicName = "";
    private boolean verbose = true;
    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    //-------------------------------------------------------------------

    public void init ( boolean bVerbose, String bootstrapServer, String topicName)
    {
        this.verbose = bVerbose;
        this.bootstrapServer = bootstrapServer;
        this.topicName = topicName;
    }

    //-------------------------------------------------------------------

    private void print(String data) {
        if (this.verbose) System.out.println(data);
    }

    //-------------------------------------------------------------------

    private int getSensorValue(int client) {
        Random r = new Random();
        int randomInt = r.nextInt(10) + 1 + client*100;
        return randomInt;
    }

    //--------------------------------------------------------------
    @Override
    public void run() {

        // Initialize producer
        final Properties props_producer = new Properties();
        props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        props_producer.put(ProducerConfig.CLIENT_ID_CONFIG, System.getProperty("user.name"));
        props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props_producer.put(ProducerConfig.BATCH_SIZE_CONFIG, "1000");
        boolean bStop = false;
        int nbKeys = 5;

        final Producer<String, String> producer = new KafkaProducer<>(props_producer);

        this.print(dateFormat.format(new Date()) + ">Data generation starting");
        StringBuilder dataBuilder = new StringBuilder("\"data\": \"");

        int i = 0;
        while (!bStop) {
            try {
                String key = "Key" + (i % nbKeys);
                String record = "{ \"SensorId\": \"" + key + "\", \"datetime\": \"" + dateFormat.format(new Date() ) + "\",\"SensorValue\": "+ this.getSensorValue (i % nbKeys)+ "}";

                this.print( ">Produced " + key+ ":"+record);
                if (i == 5000) bStop = true;

                if (i % nbKeys == 0)
                {
                    Thread.sleep( 1000);
                }

                final ProducerRecord<String, String> pr = new ProducerRecord<>(this.topicName, key, record);
                producer.send(pr);
                i++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.print(dateFormat.format(new Date()) + ">Data generation done");
        producer.close();


    }

    //--------------------------------------------------------------
}
