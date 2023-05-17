package com.example.springkafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

  @KafkaListener(groupId = "demo", topics = "demo", containerFactory = "demoKafkaListenerContainerFactory")
  public void listen(String message) {
    log.info("Received Message: " + message);
  }

  @KafkaListener(
      topicPartitions = @TopicPartition(topic = "demo",
          partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}),
      containerFactory = "partitionsKafkaListenerContainerFactory")
  public void listenWithHeaders(
      @Payload String message,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    log.info("Received Message: " + message + " from partition: " + partition);
  }
}
