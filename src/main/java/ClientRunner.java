import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by sinash on 3/28/17.
 */
public class ClientRunner {


    public static void main(String[] args) {

        HashMap<String, Integer> custCountryMap = new HashMap<String, Integer>();

        final Logger log = Logger.getLogger(ClientRunner.class.getName());

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
                    if (custCountryMap.containsValue(record.value())) {
                        updatedCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updatedCount);
                    JSONObject json = new JSONObject(custCountryMap);
                    System.out.println(json.toString());
                }
                if (args[1].equalsIgnoreCase("sync")) {

                    try {
                        consumer.commitSync(); //This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is encountered
                    } catch (CommitFailedException e) {
                        log.error("Commit Failed, " + e);
                    }
                } else if (args[1].equalsIgnoreCase("async")) {
                    try {
                        consumer.commitAsync();
                    } catch (CommitFailedException e) {
                        log.error("Commit Failed, " + e);
                    }
                } else if (args[1].equalsIgnoreCase("async-callback")) {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {

                                log.error("Commit Failed for Offsets {" + offsets + "}" + exception);

                            }
                        }
                    });
                }

            }

        } finally {
            try {
                consumer.commitSync(); //make sure the commits are received by the broker before shutdown
            } finally {
                consumer.close();
            }
        }

    }

}
