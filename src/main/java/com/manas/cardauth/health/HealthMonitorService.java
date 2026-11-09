package com.manas.cardauth.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Consumes the health-monitor's authorization/reversal/clearing stream off
 * Kafka (claim 2) and feeds every event through the per-BIN rolling-window
 * aggregator; a window close is handed to {@link IncidentDetector}, and any
 * resulting incident is stored, together with the exact window that
 * triggered it, in {@link IncidentStore} for the React console to open
 * (claim 7).
 */
@Component
public class HealthMonitorService {

    private final IncidentDetector detector;
    private final IncidentStore store;
    private final BinWindowAggregator aggregator;

    public HealthMonitorService(IncidentStore store,
                                 @Value("${cardauth.health.window-seconds:10}") long windowSeconds,
                                 @Value("${cardauth.health.minimum-volume:50}") int minimumVolume,
                                 @Value("${cardauth.health.approval-rate-floor:0.80}") double approvalRateFloor) {
        this.store = store;
        this.detector = new IncidentDetector(minimumVolume, approvalRateFloor);
        this.aggregator = new BinWindowAggregator(Duration.ofSeconds(windowSeconds), this::onWindowClose);
    }

    @KafkaListener(
            topics = "${cardauth.health.kafka.topic:card-authorization-health-stream}",
            containerFactory = "healthKafkaListenerContainerFactory")
    public void onEvent(AuthEvent event) {
        aggregator.record(event);
    }

    private void onWindowClose(WindowSummary summary) {
        detector.evaluate(summary).ifPresent(incident -> store.add(incident, summary));
    }

    /** Flushes every still-open window; used by tests and by a clean shutdown. */
    public void flush() {
        aggregator.flush();
    }
}
