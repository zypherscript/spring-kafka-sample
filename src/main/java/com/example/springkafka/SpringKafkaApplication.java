package com.example.springkafka;

import java.util.concurrent.CompletableFuture;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@SpringBootApplication
public class SpringKafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }

  @Profile("test")
  @Bean
  public ApplicationRunner runner(KafkaTemplate<String, String> template) {
    return args -> {
      String message = "test";
      CompletableFuture<SendResult<String, String>> future = template.send("demo", message);
      future.whenComplete((result, ex) -> {
        if (ex == null) {
          System.out.println(
              "Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset()
                  + "]");
        } else {
          System.out.println(
              "Unable to send message=[" + message + "] due to : " + ex.getMessage());
        }
      });
    };
  }

  @KafkaListener(groupId = "demo", topics = "demo", containerFactory = "demoKafkaListenerContainerFactory")
  public void listen(String message) {
    System.out.println("Received Message: " + message);
  }

  @KafkaListener(
      topicPartitions = @TopicPartition(topic = "demo",
          partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}),
      containerFactory = "partitionsKafkaListenerContainerFactory")
  public void listenWithHeaders(
      @Payload String message,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
    System.out.println("Received Message: " + message + " from partition: " + partition);
  }
}
