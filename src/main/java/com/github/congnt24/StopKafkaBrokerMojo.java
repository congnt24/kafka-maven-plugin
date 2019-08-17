package com.github.congnt24;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "stop-kafka", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST)
public class StopKafkaBrokerMojo extends AbstractMojo {

    @Parameter(defaultValue = "9092")
    private Integer kafkaPort;

    @Parameter(defaultValue = "2181")
    private Integer zookeeperPort;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Attempting to stop the Kafka broker");
        KafkaStandalone.getInstance().tearDown();
    }
}