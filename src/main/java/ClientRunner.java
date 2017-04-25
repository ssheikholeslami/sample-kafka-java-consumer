import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Created by sinash on 3/28/17.
 */
public class ClientRunner {

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        HashMap<String, Integer> custCountryMap = new HashMap<String, Integer>();

        final Logger log = Logger.getLogger(ClientRunner.class.getName());

        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");

        kafkaProps.put("group.id", "GroupId");

        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(kafkaProps);

        //subscribe to the topic
        consumer.subscribe(Collections.singletonList(args[0]), new RebalanceHandler());


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

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                }
                if (args[1].equalsIgnoreCase("sync")) {

                    try {
                        consumer.commitSync(currentOffsets); //This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is encountered
                    } catch (CommitFailedException e) {
                        log.error("Commit Failed, " + e);
                    }
                } else if (args[1].equalsIgnoreCase("async")) {
                    try {
                        consumer.commitAsync();
                    } catch (Exception e) {
                        log.error("Commit Failed, " + e);
                    }
                } else if (args[1].equalsIgnoreCase("async-callback")) {

                    consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
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
                consumer.commitSync(currentOffsets); //make sure the commits are received by the broker before shutdown
            } finally {
                consumer.close();
            }
        }

    }

    private static class RebalanceHandler implements ConsumerRebalanceListener {

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            consumer.commitSync(currentOffsets);

        }
    }

}
