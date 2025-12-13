package com.streamflow.core.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@Service
public class NotificationService {

    private static final String TOPIC = "user-notifications";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NotificationRepository repository;

    // --- 1. PRODUCER (The Sender) ---
    // This sends the message to Kafka. It returns IMMEDIATELY (Async).
    // The user doesn't wait for the email to actually be sent.
    public String sendNotification(String message) {
        kafkaTemplate.send(TOPIC, message);
        return "Message sent to Kafka topic: " + message;
    }

    // --- 2. CONSUMER (The Worker) ---
    // This listens to Kafka silently in the background.
    // When a message arrives, it wakes up and saves it to MongoDB.
    @KafkaListener(topics = TOPIC, groupId = "notification-group")
    public void consume(String message) {
        System.out.println("🔥 Kafka Listener received: " + message);

        // Save to MongoDB
        NotificationLog log = new NotificationLog();
        log.setMessage(message);
        log.setTimestamp(LocalDateTime.now());
        repository.save(log);
        
        System.out.println("✅ Saved to MongoDB!");
    }
}

// --- SUPPORTING CLASSES (Put these in the same file for speed) ---

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