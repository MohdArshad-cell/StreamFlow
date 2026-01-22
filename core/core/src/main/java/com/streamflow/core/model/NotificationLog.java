package com.streamflow.core.model;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "logs")
@Data
@NoArgsConstructor
public class NotificationLog {

    @Id
    private String id;

    private String message;

    // New fields for richer domain
    private String type;       // INFO, WARN, ERROR
    private String channel;    // EMAIL, SMS, PUSH, etc.
    private String userId;     // optional: who this belongs to

    private LocalDateTime timestamp;

    @Builder
    public NotificationLog(String message,
                           String type,
                           String channel,
                           String userId,
                           LocalDateTime timestamp) {
        this.message = message;
        this.type = type;
        this.channel = channel;
        this.userId = userId;
        this.timestamp = (timestamp != null) ? timestamp : LocalDateTime.now();
    }

    public static NotificationLog fromMessage(String message) {
        return NotificationLog.builder()
                .message(message)
                .type("INFO")
                .channel("SYSTEM")
                .userId(null)
                .timestamp(LocalDateTime.now())
                .build();
    }
}
