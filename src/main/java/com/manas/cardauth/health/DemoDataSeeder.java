package com.manas.cardauth.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Populates {@link IncidentStore} at startup with a small, fast, in-process
 * run of {@link HealthStreamGenerator} (a few thousand events, not the
 * 2,000,000-message measurement run) so the React console has real
 * incidents to open the moment the app starts, without waiting on Kafka or
 * on the full benchmark scale. The measured claims in the README come from
 * {@code HealthBenchmarkRunner}'s separate, dedicated, much larger run; this
 * seeder exists only to make the live console demonstrable.
 */
@Component
public class DemoDataSeeder implements ApplicationRunner {

    private final IncidentStore store;
    private final long windowSeconds;
    private final int minimumVolume;
    private final double approvalRateFloor;

    public DemoDataSeeder(IncidentStore store,
                           @Value("${cardauth.health.window-seconds:10}") long windowSeconds,
                           @Value("${cardauth.health.minimum-volume:50}") int minimumVolume,
                           @Value("${cardauth.health.approval-rate-floor:0.80}") double approvalRateFloor) {
        this.store = store;
        this.windowSeconds = windowSeconds;
        this.minimumVolume = minimumVolume;
        this.approvalRateFloor = approvalRateFloor;
    }

    @Override
    public void run(ApplicationArguments args) {
        HealthStreamGenerator generator = new HealthStreamGenerator();
        // 2,000 authorizations per BIN over a 400s span averages 5/sec per BIN, comfortably
        // clearing the default 50-event minimum-volume guard inside each 10s window.
        HealthStreamGenerator.GeneratedHealthStream demo = generator.generate(
                10, 2_000, 0.97, 0.15, 4, Duration.ofSeconds(20), Duration.ofSeconds(400), 2026_09_01L);

        IncidentDetector detector = new IncidentDetector(minimumVolume, approvalRateFloor);
        BinWindowAggregator aggregator = new BinWindowAggregator(Duration.ofSeconds(windowSeconds),
                summary -> detector.evaluate(summary).ifPresent(incident -> store.add(incident, summary)));

        for (AuthEvent event : demo.events()) {
            aggregator.record(event);
        }
        aggregator.flush();
    }
}
