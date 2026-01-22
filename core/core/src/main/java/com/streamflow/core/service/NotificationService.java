package com.streamflow.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.core.config.NotificationProperties;
import com.streamflow.core.dto.NotificationRequest;
import com.streamflow.core.dto.NotificationResponse;
import com.streamflow.core.dto.NotificationStatsResponse;
import com.streamflow.core.model.NotificationLog;
import com.streamflow.core.repository.NotificationRepository;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NotificationRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final NotificationProperties properties;
    private final MetricsService metricsService;
    private final ObjectMapper objectMapper; // <--- ADD THIS

    public NotificationService(KafkaTemplate<String, String> kafkaTemplate,
                               NotificationRepository repository,
                               StringRedisTemplate redisTemplate,
                               NotificationProperties properties,
                               MetricsService metricsService,
                               ObjectMapper objectMapper) { // <--- Inject ObjectMapper
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
    }

    // ========== WRITE PATH (PRODUCER) ==========

    public NotificationResponse sendNotification(NotificationRequest request) {
        String topic = properties.getKafka().getMainTopic();
        
        try {
            // FIX: Serialize the WHOLE request object to JSON
            String payload = objectMapper.writeValueAsString(request);
            
            kafkaTemplate.send(topic, payload);
            metricsService.incrementNotificationsSent();
            log.info("Sent payload to Kafka topic '{}': {}", topic, payload);

            return NotificationResponse.builder()
                    .status("QUEUED")
                    .message(request.getMessage())
                    .type(request.getType())
                    .channel(request.getChannel())
                    .userId(request.getUserId())
                    .detail("Notification queued successfully")
                    .queuedAt(LocalDateTime.now())
                    .build();
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize notification request", e);
            throw new RuntimeException("Serialization error");
        }
    }

    // ========== CONSUMER (RESILIENT WORKER) ==========

    @KafkaListener(
            topics = "#{notificationProperties.kafka.mainTopic}",
            groupId = "#{notificationProperties.kafka.consumerGroup}"
    )
    @Retryable(
            retryFor = RuntimeException.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void consume(String payload) {
        Timer.Sample sample = metricsService.startTimer();
        
        try {
            log.info("Processing payload: {}", payload);

            // Simulation of failure
            if (payload.contains("error")) {
                throw new RuntimeException("Simulated API Failure!");
            }

            // FIX: Deserialize JSON back to Object
            NotificationRequest request = objectMapper.readValue(payload, NotificationRequest.class);

            // Step A: Persistent Storage (MongoDB)
            // Now we map ALL fields (userId, type, channel)
            NotificationLog entity = new NotificationLog(
                    request.getMessage(),
                    request.getType(),
                    request.getChannel(),
                    request.getUserId(),
                    LocalDateTime.now()
            );
            
            repository.save(entity);
            log.info("Saved to MongoDB: [User: {}, Type: {}]", request.getUserId(), request.getType());

            // Step B: Performance Cache (Redis)
            String redisKey = properties.getRedis().getRecentNotificationsKey();
            int limit = properties.getRedis().getRecentNotificationsLimit();

            // We cache the full JSON payload so the "Recent" endpoint returns rich data too
            redisTemplate.opsForList().leftPush(redisKey, payload);
            redisTemplate.opsForList().trim(redisKey, 0, limit - 1);

            metricsService.incrementNotificationsProcessed();
            metricsService.stopTimer(sample);
            
        } catch (Exception e) {
            log.error("Consumer error", e);
            metricsService.incrementNotificationsFailed();
            throw new RuntimeException(e); // Trigger retry
        }
    }

    // ========== FALLBACK (RECOVER) ==========

    @Recover
    public void recover(RuntimeException e, String payload) {
        log.error("All retries failed. Sending to DLQ: {}", payload);
        String dlqTopic = properties.getKafka().getDlqTopic();
        kafkaTemplate.send(dlqTopic, "FAILED_PAYLOAD: " + payload);
        metricsService.incrementDlqMessages();
    }

    // ========== DLQ LISTENER ==========

    @KafkaListener(
            topics = "#{notificationProperties.kafka.dlqTopic}",
            groupId = "#{notificationProperties.kafka.dlqConsumerGroup}"
    )
    public void consumeDLQ(String message) {
        log.warn("DLQ Analysis Required: {}", message);
    }

    // ========== READ PATHS ==========

    public List<String> getRecentNotifications() {
        String redisKey = properties.getRedis().getRecentNotificationsKey();
        // Returns list of JSON strings
        return redisTemplate.opsForList().range(redisKey, 0, -1);
    }

    // ... (Keep your existing Mongo query methods below: getNotificationHistory, getNotificationsByType, etc.)
    
    public Page<NotificationLog> getNotificationHistory(Pageable pageable) {
        return repository.findAllByOrderByTimestampDesc(pageable);
    }

    public List<NotificationLog> getNotificationsByType(String type) {
        return repository.findByTypeOrderByTimestampDesc(type);
    }

    public List<NotificationLog> getNotificationsByUser(String userId) {
        return repository.findByUserIdOrderByTimestampDesc(userId);
    }

    public List<NotificationLog> getNotificationsByChannel(String channel) {
        return repository.findByChannelOrderByTimestampDesc(channel);
    }

    public List<NotificationLog> getNotificationsByTimeRange(LocalDateTime start, LocalDateTime end) {
        return repository.findByTimestampBetweenOrderByTimestampDesc(start, end);
    }

    public NotificationStatsResponse getNotificationStats() {
        long total = repository.count();
        long info = repository.countByType("INFO");
        long warn = repository.countByType("WARN");
        long error = repository.countByType("ERROR");

        return NotificationStatsResponse.builder()
                .totalNotifications(total)
                .infoCount(info)
                .warnCount(warn)
                .errorCount(error)
                .build();
    }
}