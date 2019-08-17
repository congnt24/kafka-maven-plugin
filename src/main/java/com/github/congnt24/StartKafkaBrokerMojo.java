package com.github.congnt24;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "start-kafka", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartKafkaBrokerMojo extends AbstractMojo {
    @Parameter(defaultValue = "9092")
    private Integer kafkaPort;

    @Parameter(defaultValue = "2181")
    private Integer zookeeperPort;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            getLog().info("Starting Zookeeper on port " + zookeeperPort + " and Kafka broker on port " + kafkaPort);
            KafkaStandalone.getInstance().prepare(kafkaPort, zookeeperPort);
        } catch (Exception e) {
            getLog().error("Failed to start Kafka broker", e);
            throw new MojoExecutionException("Failed to start Kafka broker");
        }
    }
}