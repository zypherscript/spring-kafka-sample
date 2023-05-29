package com.example.springkafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  public ConsumerFactory<String, String> consumerFactory(String groupId) {
    var props = getDefaultProducerConfig(groupId);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  public ConsumerFactory<String, Greeting> greetingConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
        getDefaultProducerConfig(null),
        new StringDeserializer(),
        new JsonDeserializer<>(Greeting.class));
  }

  private Map<String, Object> getDefaultProducerConfig(String groupId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    if (groupId != null) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      String groupId) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory(groupId));
//    factory.setRecordFilterStrategy(record -> record.value().contains("filter txt"));
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Greeting>
  greetingKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Greeting> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(greetingConsumerFactory());
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> partitionsKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory("partitions");
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> demoKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory("demo");
  }

}
