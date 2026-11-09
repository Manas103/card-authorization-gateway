package com.manas.cardauth.health;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * REST surface for the React incident console: a list view and a detail
 * view that opens an incident directly on its triggering segment and
 * decline code (claim 7).
 */
@RestController
@RequestMapping("/api/health")
public class HealthController {

    private final IncidentStore store;

    public HealthController(IncidentStore store) {
        this.store = store;
    }

    @GetMapping("/incidents")
    public List<Incident> incidents() {
        return store.all().stream()
                .sorted(Comparator.comparing(Incident::detectedAt).reversed())
                .toList();
    }

    @GetMapping("/incidents/{id}")
    public ResponseEntity<Map<String, Object>> incidentDetail(@PathVariable long id) {
        Incident incident = store.byId(id);
        if (incident == null) {
            return ResponseEntity.notFound().build();
        }
        WindowSummary segment = store.triggeringSegmentOf(id);
        return ResponseEntity.ok(Map.of(
                "incident", incident,
                "triggeringSegment", Map.of(
                        "issuerBin", segment.issuerBin(),
                        "windowStart", segment.windowStart(),
                        "windowEnd", segment.windowEnd(),
                        "totalAuthorizations", segment.totalAuthorizations(),
                        "approvedCount", segment.approvedCount(),
                        "declinedCount", segment.declinedCount(),
                        "declineCodeCounts", segment.declineCodeCounts(),
                        "approvalRate", segment.approvalRate(),
                        "decisionLatencyP50Nanos", segment.decisionLatencyP50Nanos(),
                        "decisionLatencyP99Nanos", segment.decisionLatencyP99Nanos()
                )
        ));
    }
}
