package com.streamflow.core.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class MetricsService {

    private final Counter notificationsSentCounter;
    private final Counter notificationsProcessedCounter;
    private final Counter notificationsFailedCounter;
    private final Counter dlqMessagesCounter;
    private final Timer processingTimer;

    public MetricsService(MeterRegistry meterRegistry) {
        // Counter for notifications sent to Kafka
        this.notificationsSentCounter = Counter.builder("notifications.sent.total")
                .description("Total number of notifications sent to Kafka")
                .register(meterRegistry);

        // Counter for successfully processed notifications
        this.notificationsProcessedCounter = Counter.builder("notifications.processed.total")
                .description("Total number of notifications processed successfully")
                .register(meterRegistry);

        // Counter for failed notifications
        this.notificationsFailedCounter = Counter.builder("notifications.failed.total")
                .description("Total number of notifications that failed processing")
                .register(meterRegistry);

        // Counter for DLQ messages
        this.dlqMessagesCounter = Counter.builder("notifications.dlq.total")
                .description("Total number of messages sent to DLQ")
                .register(meterRegistry);

        // Timer for processing duration
        this.processingTimer = Timer.builder("notifications.processing.time")
                .description("Time taken to process notifications")
                .register(meterRegistry);
    }

    public void incrementNotificationsSent() {
        notificationsSentCounter.increment();
    }

    public void incrementNotificationsProcessed() {
        notificationsProcessedCounter.increment();
    }

    public void incrementNotificationsFailed() {
        notificationsFailedCounter.increment();
    }

    public void incrementDlqMessages() {
        dlqMessagesCounter.increment();
    }

    public void recordProcessingTime(long duration, TimeUnit unit) {
        processingTimer.record(duration, unit);
    }

    public Timer.Sample startTimer() {
        return Timer.start();
    }

    public void stopTimer(Timer.Sample sample) {
        sample.stop(processingTimer);
    }
}
