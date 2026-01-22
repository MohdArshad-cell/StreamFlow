package com.streamflow.core;

import com.streamflow.core.dto.NotificationRequest;
import com.streamflow.core.model.NotificationLog;
import com.streamflow.core.repository.NotificationRepository;
import com.streamflow.core.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Testcontainers
class CoreApplicationTests {

    // 1. Spin up real MongoDB (ver 6.0)
    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:6.0");

    // 2. Spin up real Kafka (Confluent 7.4)
    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    // 3. Inject Container URLs into Spring Properties
    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    @Autowired
    private NotificationService service;

    @Autowired
    private NotificationRepository repository;

    @Test
    void testEndToEndNotificationFlow() {
        // GIVEN: A rich notification request
        String userId = "test-user-" + System.currentTimeMillis();
        String message = "Integration Test Message";
        
        NotificationRequest request = new NotificationRequest();
        request.setUserId(userId);
        request.setMessage(message);
        request.setType("INFO");
        request.setChannel("EMAIL");

        // WHEN: We send the request object (This serializes to JSON)
        service.sendNotification(request);

        // THEN: We wait for Kafka to consume -> Deserialize -> Save to Mongo
        await().pollInterval(Duration.ofMillis(500))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    // Check MongoDB for this specific user
                    List<NotificationLog> logs = repository.findByUserIdOrderByTimestampDesc(userId);
                    
                    assertEquals(1, logs.size(), "Should find 1 log for this user");
                    NotificationLog log = logs.get(0);
                    
                    // Verify all fields are preserved
                    assertEquals(message, log.getMessage());
                    assertEquals("INFO", log.getType());
                    assertEquals("EMAIL", log.getChannel());
                    assertNotNull(log.getTimestamp());
                });
    }
}