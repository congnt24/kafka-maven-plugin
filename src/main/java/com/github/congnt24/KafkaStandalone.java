package com.github.congnt24;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A utility class for starting/stopping kafka Server.
 */
public class KafkaStandalone {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStandalone.class);
    private static final KafkaStandalone instance = new KafkaStandalone();

    private KafkaLocal kafkaServer;
    private String kafkaServerUrl;
    private String zkServerUrl;
    private int kafkaLocalPort;
    private int zkLocalPort;
    private AdminClient adminClient;

    public static KafkaStandalone getInstance() {
        return instance;
    }

    public KafkaStandalone() {
        System.setProperty("log4j.configurationFile", getClass().getClassLoader().getResource("log4j2.xml").toString());
    }

    private boolean startEmbeddedKafkaServer() {
        Properties kafkaProperties = new Properties();
        Properties zkProperties = new Properties();

        logger.info("Starting kafka server.");
        try {
            //load properties
            zkProperties.load(getClass().getClassLoader().getResourceAsStream("zookeeper.properties"));
            //start local Zookeeper
            // override the Zookeeper client port with the generated one.
            zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
            new ZooKeeperLocal(zkProperties);
            logger.info("ZooKeeper instance is successfully started on port " + zkLocalPort);
            kafkaProperties.load(getClass().getClassLoader().getResourceAsStream("kafka-server.properties"));
            // override the Zookeeper url.
            kafkaProperties.setProperty("zookeeper.connect", getZkUrl());
            // override the kafka server port
            kafkaProperties.setProperty("port", Integer.toString(kafkaLocalPort));
            kafkaServer = new KafkaLocal(kafkaProperties);
            kafkaServer.start();
            logger.info("kafka Server is successfully started on port " + kafkaLocalPort);
            return true;

        } catch (Exception e) {
            logger.error("Error starting the kafka Server.", e);
            return false;
        }
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            Properties adminClientProps = createAdminClientProperties();
            adminClient = AdminClient.create(adminClientProps);
        }
        return adminClient;
    }

    private Properties createAdminClientProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerUrl());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_1");
        return props;
    }

    public void createTopics(List<String> topicNames, int numPartitions) {
        List<NewTopic> newTopics = new ArrayList<>();
        for (String topicName : topicNames) {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
            newTopics.add(newTopic);
        }
        getAdminClient().createTopics(newTopics);

        //the following lines are a bit of black magic to ensure the topic is ready when we return
        DescribeTopicsResult dtr = getAdminClient().describeTopics(topicNames);
        try {
            dtr.all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Error getting topic info", e);
        }
    }

    public void deleteTopic(String topicName) {
        getAdminClient().deleteTopics(Collections.singletonList(topicName));
    }

    public void prepare(int kafkaLocalPort, int zkLocalPort) {
        this.kafkaLocalPort = kafkaLocalPort;
        this.zkLocalPort = zkLocalPort;
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            kafkaServerUrl = hostname + ":" + kafkaLocalPort;
            zkServerUrl = hostname + ":" + zkLocalPort;
        } catch (Exception e) {
            logger.error("Unexpected error", e);
            throw new RuntimeException("Unexpected error", e);
        }
        boolean startStatus = startEmbeddedKafkaServer();
        if (!startStatus) {
            throw new RuntimeException("Error starting the server!");
        }
        try {
            Thread.sleep(3000);   // add this sleep time to
        } catch (InterruptedException e) {
        }
        logger.info("Completed the prepare phase.");
    }

    public void tearDown() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
        }
        if (kafkaServer != null) {
            logger.info("Shutting down the kafka Server.");
            kafkaServer.stop();
        }
        logger.info("Completed the tearDown phase.");
    }

    public String getZkUrl() {
        return zkServerUrl;
    }

    public String getKafkaServerUrl() {
        return kafkaServerUrl;
    }
}