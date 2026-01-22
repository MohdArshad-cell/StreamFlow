package com.streamflow.core.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class NotificationResponse {
    private String id;
    private String status;
    private String message;
    private String type;        // ← ADD THIS
    private String channel;     // ← ADD THIS
    private String userId;      // ← ADD THIS
    private String detail;
    private LocalDateTime queuedAt;
}
