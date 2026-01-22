package com.streamflow.core.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NotificationStatsResponse {
    private long totalNotifications;
    private long infoCount;
    private long warnCount;
    private long errorCount;
}
