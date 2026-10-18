package com.example.eventprocessor;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
public abstract class AbstractPostgresIntegrationTest {

    protected static final PostgreSQLContainer<?> postgres;
    protected static final KafkaContainer kafka;
    protected static final GenericContainer<?> redis;

    static {
        System.setProperty("api.version", "1.44");
        System.setProperty("docker.client.apiVersion", "1.44");
        System.setProperty("testcontainers.ryuk.disabled", "true");

        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();

        kafka = new KafkaContainer("apache/kafka:3.7.0");
        kafka.start();

        redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
        redis.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    public static void pauseRedis() {
        redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
    }

    public static void unpauseRedis() {
        redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
    }
}
