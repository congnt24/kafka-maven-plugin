package com.github.congnt24;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.IOException;
import java.util.Properties;

/**
 * A local kafka server for running unit tests.
 * Reference: https://gist.github.com/fjavieralba/7930018/
 */
public class KafkaLocal {

    public KafkaServerStartable kafka;
    public ZooKeeperLocal zookeeper;

    public KafkaLocal(Properties kafkaProperties) throws IOException, InterruptedException {
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(kafkaProperties);

        // start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
    }

    public void start() throws Exception {
        kafka.startup();
    }

    public void stop() {
        kafka.shutdown();
    }

}