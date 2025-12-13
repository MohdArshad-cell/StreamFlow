package com.streamflow.core.service;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@Service
public class NotificationService {

    private static final String TOPIC = "user-notifications";
    private static final String DLQ_TOPIC = "notifications-dlq";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NotificationRepository repository;

    // --- 1. PRODUCER ---
    public String sendNotification(String message) {
        kafkaTemplate.send(TOPIC, message);
        return "Message sent to Kafka topic: " + message;
    }

    // --- 2. CONSUMER (Resilient Worker) ---
    // Tries 3 times. Waits 1s, then 2s. If all fail, calls recover().
    @KafkaListener(topics = TOPIC, groupId = "notification-group")
    @Retryable(
        retryFor = RuntimeException.class, 
        maxAttempts = 3, 
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void consume(String message) {
        System.out.println("🔄 Processing Attempt: " + message);

        if (message.contains("error")) {
            throw new RuntimeException("Simulated API Failure!");
        }

        NotificationLog log = new NotificationLog();
        log.setMessage(message);
        log.setTimestamp(LocalDateTime.now());
        repository.save(log);
        System.out.println("✅ Saved to MongoDB!");
    }

    // --- 3. FALLBACK (Recover Method) ---
    // Only runs if ALL retries fail. Moves data to DLQ.
    @Recover
    public void recover(RuntimeException e, String message) {
        System.err.println("❌ All Retries Failed. Moving to DLQ...");
        kafkaTemplate.send(DLQ_TOPIC, "FAILED: " + message);
    }

    // --- 4. DLQ LISTENER ---
    @KafkaListener(topics = DLQ_TOPIC, groupId = "dlq-group")
    public void consumeDLQ(String message) {
        System.out.println("⚠️ DLQ Received Bad Message: " + message);
    }
}

// --- SUPPORTING CLASSES ---
@RestController
@RequestMapping("/api/notify")
class NotificationController {
    @Autowired
    private NotificationService service;

    @PostMapping
    public String trigger(@RequestParam String message) {
        return service.sendNotification(message);
    }
}

@Document(collection = "logs")
@Data
@NoArgsConstructor
class NotificationLog {
    @Id
    private String id;
    private String message;
    private LocalDateTime timestamp;
}

interface NotificationRepository extends MongoRepository<NotificationLog, String> {}