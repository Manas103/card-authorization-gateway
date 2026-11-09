package com.manas.cardauth.health;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory incidents the React console reads, populated by
 * {@link HealthMonitorService} as windows close. Also keeps the window
 * summary each incident was raised from, so the console can open an
 * incident on its exact triggering segment (claim: "opens each incident on
 * its triggering segment and decline code").
 */
@Component
public final class IncidentStore {

    private final List<Incident> incidents = new CopyOnWriteArrayList<>();
    private final Map<Long, WindowSummary> triggeringSegments = new ConcurrentHashMap<>();

    public void add(Incident incident, WindowSummary triggeringSegment) {
        incidents.add(incident);
        triggeringSegments.put(incident.id(), triggeringSegment);
    }

    public List<Incident> all() {
        return List.copyOf(incidents);
    }

    public Incident byId(long id) {
        return incidents.stream().filter(i -> i.id() == id).findFirst().orElse(null);
    }

    public WindowSummary triggeringSegmentOf(long incidentId) {
        return triggeringSegments.get(incidentId);
    }

    public void clear() {
        incidents.clear();
        triggeringSegments.clear();
    }
}
