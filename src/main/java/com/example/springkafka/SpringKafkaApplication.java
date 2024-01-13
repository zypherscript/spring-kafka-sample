package com.example.springkafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
@Slf4j
public class SpringKafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }

  @Profile("test")
  @Bean
  public ApplicationRunner runner(KafkaProducer kafkaProducer) {
    return args -> {
      kafkaProducer.send("demo", "test");
      kafkaProducer.greeting("demo-greeting", new Greeting("testMsg", "testName"));
    };
  }
}
