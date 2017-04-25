import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by sinash on 3/28/17.
 */
public class ClientRunner {


    public static void main(String[] args) {

        HashMap<String, Integer> custCountryMap = new HashMap<String, Integer>();

        Logger log = Logger.getLogger(ClientRunner.class.getName());

        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");

        kafkaProps.put("group.id", "GroupId");

        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);

        //subscribe to the topic
        consumer.subscribe(Collections.singletonList(args[0]));


        // The Poll loop
        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100); // keep polling or you'll be considered DEAD
                for (ConsumerRecord<String, String> record : records) {
                    log.debug("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset() + ", customer = " + record.key() + ", country = " + record.value());

                    int updatedCount = 1;
                    if (custCountryMap.containsValue(record.value())){
                        updatedCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updatedCount);
                    JSONObject json = new JSONObject(custCountryMap);
                    System.out.println(json.toString());
                }
            }
        }
        finally {
            consumer.close();
        }

    }

}
