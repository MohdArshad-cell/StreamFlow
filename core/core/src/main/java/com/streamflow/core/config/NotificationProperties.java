package com.streamflow.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "notification")
@Data
public class NotificationProperties {
    
    private Kafka kafka = new Kafka();
    private Redis redis = new Redis();
    
    @Data
    public static class Kafka {
        private String mainTopic = "user-notifications";
        private String dlqTopic = "notifications-dlq";
        private String consumerGroup = "notification-group";
        private String dlqConsumerGroup = "dlq-group";
    }
    
    @Data
    public static class Redis {
        private String recentNotificationsKey = "recent_notifications";
        private int recentNotificationsLimit = 10;
    }
}
