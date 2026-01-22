package com.streamflow.core.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "Request to send a notification")
public class NotificationRequest {

    @Schema(description = "Notification message content", example = "User order confirmed", required = true)
    @NotBlank(message = "Message must not be blank")
    private String message;

    @Schema(description = "Notification type", example = "INFO", allowableValues = {"INFO", "WARN", "ERROR"})
    private String type;

    @Schema(description = "Notification channel", example = "EMAIL", allowableValues = {"EMAIL", "SMS", "PUSH", "SYSTEM"})
    private String channel;

    @Schema(description = "Target user ID", example = "user-123")
    private String userId;
}
