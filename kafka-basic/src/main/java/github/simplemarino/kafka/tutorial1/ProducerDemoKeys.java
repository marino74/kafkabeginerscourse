package github.simplemarino.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i<10; i++){
            // Create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World "+i);

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a record is successfully sent or an Exception is thrown
                    if (e==null){
                        // The record was successfully sent
                        logger.info("Received new metadata: \n"+
                                "Topic:"+recordMetadata.topic()+"\n"+
                                "Partition:"+recordMetadata.partition()+"\n"+
                                "Offset:"+recordMetadata.offset()+"\n"+
                                "Timestamp:"+recordMetadata.timestamp());
                    }else {
                        logger.error("Error while producing", e);
                    }
                }
            });

            // flush data
            producer.flush();

            // flush and close producer
            producer.close();
        }
    }
}
