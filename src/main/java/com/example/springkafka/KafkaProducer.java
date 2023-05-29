package com.example.springkafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaTemplate<String, Greeting> greetingKafkaTemplate;

  public void send(String topic, String message) {
    kafkaTemplate.send(topic, message)
        .whenComplete((result, ex) -> logging(ex, message, result.getRecordMetadata().offset()));
  }

  public void greeting(String topic, Greeting greeting) {
    greetingKafkaTemplate.send(topic, greeting)
        .whenComplete(
            (result, ex) -> logging(ex, greeting.toString(), result.getRecordMetadata().offset()));
  }

  private void logging(Throwable ex, String message, long offset) {
    if (ex == null) {
      log.info(
          "Sent message=[" + message + "] with offset=[" + offset
              + "]");
    } else {
      log.error(
          "Unable to send message=[" + kafkaTemplate + "] due to : " + ex.getMessage());
    }
  }
}
