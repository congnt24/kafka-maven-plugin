package com.github.congnt24;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class KafkaTest {

    @BeforeAll
    static void setUp() {
        KafkaStandalone.getInstance().prepare(9092, 2181);
    }

    @Test
    void name() {
        System.out.println("Kafka start in port: " + KafkaStandalone.getInstance().getKafkaServerUrl());
//        Create topic
        KafkaStandalone.getInstance().createTopics(Collections.singletonList("test_topic"), 1);
    }

    @AfterAll
    static void tearDown() {
        KafkaStandalone.getInstance().tearDown();
    }
}
