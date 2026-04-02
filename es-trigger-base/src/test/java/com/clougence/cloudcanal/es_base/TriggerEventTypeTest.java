package com.clougence.cloudcanal.es_base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TriggerEventTypeTest {

    @Test
    void shouldExposeNewMetadataEventCodes() {
        assertEquals(TriggerEventType.UPDATE_MAPPING, TriggerEventType.getEventType("UM"));
        assertEquals(TriggerEventType.UPDATE_SETTINGS, TriggerEventType.getEventType("US"));
        assertEquals(TriggerEventType.UPDATE_ALIASES, TriggerEventType.getEventType("UA"));
        assertTrue(TriggerEventType.UPDATE_MAPPING.isDdl());
        assertTrue(TriggerEventType.UPDATE_SETTINGS.isDdl());
        assertTrue(TriggerEventType.UPDATE_ALIASES.isDdl());
    }
}
