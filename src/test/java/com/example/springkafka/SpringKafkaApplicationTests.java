package com.example.springkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092",
    "port=9092"})
class SpringKafkaApplicationTests {

  @SpyBean
  private KafkaConsumer kafkaConsumer;

  @Captor
  private ArgumentCaptor<String> argumentCaptor;

  @Autowired
  private KafkaProducer kafkaProducer;

  private final String message = "test";

  @BeforeEach
  void setUp() {
    kafkaProducer.send("demo", message);
  }

  @Test
  void testKafkaConsumer() {
    verify(kafkaConsumer, timeout(5000)).listenWithHeaders(argumentCaptor.capture(),
        anyInt());
    assertThat(argumentCaptor.getValue()).isEqualTo(message);

    verify(kafkaConsumer, timeout(5000)).listen(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue()).isEqualTo(message);
  }

}
