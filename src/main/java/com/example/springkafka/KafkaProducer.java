package com.example.springkafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaProducer {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  public void send(String topic, String message) {
    kafkaTemplate.send(topic, message)
        .whenComplete((result, ex) -> {
          if (ex == null) {
            log.info(
                "Sent message=[" + message + "] with offset=[" + result.getRecordMetadata()
                    .offset()
                    + "]");
          } else {
            log.error(
                "Unable to send message=[" + kafkaTemplate + "] due to : " + ex.getMessage());
          }
        });
  }
}
