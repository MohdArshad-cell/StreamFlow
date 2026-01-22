package com.streamflow.core.controller;

import com.streamflow.core.dto.NotificationRequest;
import com.streamflow.core.dto.NotificationResponse;
import com.streamflow.core.dto.NotificationStatsResponse;
import com.streamflow.core.model.NotificationLog;
import com.streamflow.core.service.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1/notify")
@Tag(name = "Notifications", description = "Notification management APIs")
@SecurityRequirement(name = "API Key")
public class NotificationController {

    private final NotificationService service;

    public NotificationController(NotificationService service) {
        this.service = service;
    }

    @Operation(
            summary = "Send a notification",
            description = "Queue a notification for async processing via Kafka. The notification will be persisted to MongoDB and cached in Redis."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Notification queued successfully",
                    content = @Content(schema = @Schema(implementation = NotificationResponse.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request data"),
            @ApiResponse(responseCode = "401", description = "Unauthorized - Invalid API key")
    })
    @PostMapping
    public ResponseEntity<NotificationResponse> trigger(
            @Valid @RequestBody NotificationRequest request) {

        NotificationResponse response = service.sendNotification(request);
        return ResponseEntity.ok(response);
    }

    @Operation(
            summary = "Get recent notifications",
            description = "Retrieve the most recent notifications from Redis cache (fast path)"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Recent notifications retrieved"),
            @ApiResponse(responseCode = "401", description = "Unauthorized")
    })
    @GetMapping("/recent")
    public ResponseEntity<List<String>> getRecent() {
        return ResponseEntity.ok(service.getRecentNotifications());
    }

    @Operation(
            summary = "Get notification history",
            description = "Retrieve paginated notification history from MongoDB"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "History retrieved"),
            @ApiResponse(responseCode = "401", description = "Unauthorized")
    })
    @GetMapping("/history")
    public ResponseEntity<Page<NotificationLog>> getHistory(
            @Parameter(description = "Page number (0-indexed)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "10") int size) {

        Pageable pageable = PageRequest.of(page, size);
        return ResponseEntity.ok(service.getNotificationHistory(pageable));
    }

    @Operation(
            summary = "Filter notifications by type",
            description = "Get all notifications of a specific type (INFO, WARN, ERROR)"
    )
    @GetMapping("/filter/type/{type}")
    public ResponseEntity<List<NotificationLog>> getByType(
            @Parameter(description = "Notification type", example = "INFO") @PathVariable String type) {
        return ResponseEntity.ok(service.getNotificationsByType(type));
    }

    @Operation(
            summary = "Filter notifications by user",
            description = "Get all notifications for a specific user ID"
    )
    @GetMapping("/filter/user/{userId}")
    public ResponseEntity<List<NotificationLog>> getByUser(
            @Parameter(description = "User ID") @PathVariable String userId) {
        return ResponseEntity.ok(service.getNotificationsByUser(userId));
    }

    @Operation(
            summary = "Filter notifications by channel",
            description = "Get all notifications for a specific channel (EMAIL, SMS, PUSH, SYSTEM)"
    )
    @GetMapping("/filter/channel/{channel}")
    public ResponseEntity<List<NotificationLog>> getByChannel(
            @Parameter(description = "Notification channel", example = "EMAIL") @PathVariable String channel) {
        return ResponseEntity.ok(service.getNotificationsByChannel(channel));
    }

    @Operation(
            summary = "Filter notifications by time range",
            description = "Get notifications within a specific time range"
    )
    @GetMapping("/filter/timerange")
    public ResponseEntity<List<NotificationLog>> getByTimeRange(
            @Parameter(description = "Start time (ISO 8601)", example = "2026-01-01T00:00:00")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
            @Parameter(description = "End time (ISO 8601)", example = "2026-01-31T23:59:59")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {

        return ResponseEntity.ok(service.getNotificationsByTimeRange(start, end));
    }

    @Operation(
            summary = "Get notification statistics",
            description = "Retrieve aggregate statistics about notifications (total, counts by type)"
    )
    @GetMapping("/stats")
    public ResponseEntity<NotificationStatsResponse> getStats() {
        return ResponseEntity.ok(service.getNotificationStats());
    }
}
