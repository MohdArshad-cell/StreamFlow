package com.streamflow.core.service;

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

    public NotificationService(KafkaTemplate<String, String> kafkaTemplate,
                               NotificationRepository repository,
                               StringRedisTemplate redisTemplate,
                               NotificationProperties properties,
                               MetricsService metricsService) {
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metricsService = metricsService;
    }

    // ========== WRITE PATH (PRODUCER) ==========

    public NotificationResponse sendNotification(NotificationRequest request) {
        String topic = properties.getKafka().getMainTopic();
        
        kafkaTemplate.send(topic, request.getMessage());
        metricsService.incrementNotificationsSent();  // Track metric
        log.info("Message sent to Kafka topic '{}': {}", topic, request.getMessage());

        return NotificationResponse.builder()
                .status("QUEUED")
                .message(request.getMessage())
                .type(request.getType())
                .channel(request.getChannel())
                .userId(request.getUserId())
                .detail("Notification queued successfully")
                .queuedAt(LocalDateTime.now())
                .build();
    }

    public String sendNotification(String message) {
        String topic = properties.getKafka().getMainTopic();
        kafkaTemplate.send(topic, message);
        metricsService.incrementNotificationsSent();  // Track metric
        log.info("Message sent to Kafka topic '{}': {}", topic, message);
        return "Notification queued successfully";
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
    public void consume(String message) {
        Timer.Sample sample = metricsService.startTimer();  // Start timing
        
        try {
            log.info("Processing notification message: {}", message);

            if (message.contains("error")) {
                throw new RuntimeException("Simulated API Failure!");
            }

            // Step A: Persistent Storage (MongoDB)
            NotificationLog entity = NotificationLog.fromMessage(message);
            repository.save(entity);
            log.info("Notification saved to MongoDB with id={}", entity.getId());

            // Step B: Performance Cache (Redis)
            String redisKey = properties.getRedis().getRecentNotificationsKey();
            int limit = properties.getRedis().getRecentNotificationsLimit();

            redisTemplate.opsForList().leftPush(redisKey, message);
            redisTemplate.opsForList().trim(redisKey, 0, limit - 1);
            log.info("Notification cached in Redis under key={}", redisKey);

            metricsService.incrementNotificationsProcessed();  // Track success
            metricsService.stopTimer(sample);  // Record processing time
            
        } catch (Exception e) {
            metricsService.incrementNotificationsFailed();  // Track failure
            throw e;
        }
    }

    // ========== FALLBACK (RECOVER) ==========

    @Recover
    public void recover(RuntimeException e, String message) {
        log.error("All retries failed for message='{}'. Sending to DLQ.", message, e);
        String dlqTopic = properties.getKafka().getDlqTopic();
        kafkaTemplate.send(dlqTopic, "FAILED: " + message);
        metricsService.incrementDlqMessages();  // Track DLQ
    }

    // ========== DLQ LISTENER ==========

    @KafkaListener(
            topics = "#{notificationProperties.kafka.dlqTopic}",
            groupId = "#{notificationProperties.kafka.dlqConsumerGroup}"
    )
    public void consumeDLQ(String message) {
        log.warn("DLQ received bad message: {}", message);
    }

    // ========== READ PATH (FAST - REDIS) ==========

    public List<String> getRecentNotifications() {
        String redisKey = properties.getRedis().getRecentNotificationsKey();
        return redisTemplate.opsForList().range(redisKey, 0, -1);
    }

    // ========== READ PATH (HISTORY & FILTERING - MONGO) ==========

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

    // ========== STATS & OBSERVABILITY ==========

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
