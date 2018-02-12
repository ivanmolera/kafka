import common.Constants;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String... args) {
        if (args.length == 0) {
            runProducer(100);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    private static void runProducer(int numberOfMessages) {
        final Producer<String, String> producer = createProducer();

        try {
            TestCallback callback = new TestCallback();
            for (long i = 0; i < numberOfMessages; i++) {
                ProducerRecord<String, String> data = new ProducerRecord<>(Constants.TOPIC, "key-" + i, "message-" + i);
                producer.send(data, callback);
            }
        }
        finally {
            producer.flush();
            producer.close();
        }
    }

    private static class TestCallback implements Callback {

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            }
            else {
                String message = String.format("Message sent to topic:%s  partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }
}
