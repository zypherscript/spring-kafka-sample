package com.example.springkafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    var props = getDefaultProducerConfig();
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class);
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public ProducerFactory<String, Greeting> greetingProducerFactory() {
    var props = getDefaultProducerConfig();
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        JsonSerializer.class);
    return new DefaultKafkaProducerFactory<>(props);
  }

  private Map<String, Object> getDefaultProducerConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public KafkaTemplate<String, Greeting> greetingKafkaTemplate() {
    return new KafkaTemplate<>(greetingProducerFactory());
  }
}
