package com.manas.cardauth.health;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plain unit test against {@link HealthController}, no Spring context: the
 * controller has no framework-managed state of its own beyond
 * {@link IncidentStore}, so a real instance wired to a real store is enough
 * to prove the REST contract without the cost of a full web-slice test.
 */
class HealthControllerTest {

    @Test
    void incidentsListsNewestFirst() {
        IncidentStore store = new IncidentStore();
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.DO_NOT_HONOR, 10);
        WindowSummary segment = new WindowSummary("BIN-000001", Instant.EPOCH, Instant.EPOCH.plusSeconds(10),
                60, 10, 50, codes, 0, 0, 100, 200);

        store.add(new Incident(1, "BIN-000001", Instant.EPOCH, Instant.EPOCH.plusSeconds(10),
                Instant.EPOCH.plusSeconds(10), 0.16, 60, DeclineCode.DO_NOT_HONOR), segment);
        store.add(new Incident(2, "BIN-000002", Instant.EPOCH.plusSeconds(20), Instant.EPOCH.plusSeconds(30),
                Instant.EPOCH.plusSeconds(30), 0.10, 60, DeclineCode.DO_NOT_HONOR), segment);

        HealthController controller = new HealthController(store);
        List<Incident> incidents = controller.incidents();

        assertEquals(2, incidents.size());
        assertEquals(2, incidents.get(0).id(), "most recently detected incident must come first");
    }

    @Test
    void incidentDetailIncludesTheTriggeringSegmentAndDominantDeclineCode() {
        IncidentStore store = new IncidentStore();
        Map<DeclineCode, Integer> codes = new EnumMap<>(DeclineCode.class);
        codes.put(DeclineCode.SUSPECTED_FRAUD, 40);
        codes.put(DeclineCode.INVALID_PIN, 5);
        WindowSummary segment = new WindowSummary("BIN-000042", Instant.EPOCH, Instant.EPOCH.plusSeconds(10),
                100, 55, 45, codes, 0, 0, 1000, 2000);
        store.add(new Incident(7, "BIN-000042", Instant.EPOCH, Instant.EPOCH.plusSeconds(10),
                Instant.EPOCH.plusSeconds(10), 0.55, 100, DeclineCode.SUSPECTED_FRAUD), segment);

        HealthController controller = new HealthController(store);
        ResponseEntity<Map<String, Object>> response = controller.incidentDetail(7);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        Map<String, Object> body = response.getBody();
        assertNotNull(body);
        assertEquals(7L, ((Incident) body.get("incident")).id());
        @SuppressWarnings("unchecked")
        Map<String, Object> triggeringSegment = (Map<String, Object>) body.get("triggeringSegment");
        assertEquals("BIN-000042", triggeringSegment.get("issuerBin"));
        assertEquals(40, ((Map<?, ?>) triggeringSegment.get("declineCodeCounts")).get(DeclineCode.SUSPECTED_FRAUD));
    }

    @Test
    void unknownIncidentIdReturns404() {
        HealthController controller = new HealthController(new IncidentStore());
        ResponseEntity<Map<String, Object>> response = controller.incidentDetail(999);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }
}
