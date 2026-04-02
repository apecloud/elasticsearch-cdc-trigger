package com.clougence.cloudcanal.es6.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.junit.jupiter.api.Test;

import com.clougence.cloudcanal.es_base.TriggerEventType;

class CcEs6ReplayServiceTest {

    private final CcEs6ReplayService replayService = new CcEs6ReplayService(null);

    @Test
    void shouldIgnoreInternalAndClusterManagedSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.number_of_replicas", 1);
        settings.put("index.uuid", "uuid-1");
        settings.put("index.version.created", "6082399");
        settings.put("index.creation_date", "123");
        settings.put("index.provided_name", "orders");
        settings.put("index.cdc_enabled", true);
        settings.put("index.cdc_max_scn", 100L);

        Map<String, Object> normalized = replayService.normalizeSettings(settings);

        assertEquals(1, normalized.size());
        assertEquals("1", normalized.get("index.number_of_replicas"));
    }

    @Test
    void shouldNormalizeAliasesAndDropMetaFromMappings() {
        Map<String, Object> aliasConfig = new HashMap<>();
        aliasConfig.put("is_write_index", true);
        aliasConfig.put("routing", "r1");

        Map<String, Object> aliases = new HashMap<>();
        aliases.put("orders_write", aliasConfig);

        Map<String, Object> mappingBody = new HashMap<>();
        mappingBody.put("_meta", "ignore");
        mappingBody.put("properties", new HashMap<String, Object>());

        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_doc", mappingBody);

        Map<String, Object> normalizedAliases = replayService.normalizeAliases(aliases);
        Map<String, Object> normalizedMappings = replayService.normalizeMappings(mappings);

        assertTrue(normalizedAliases.containsKey("orders_write"));
        assertEquals(true, ((Map<?, ?>) normalizedAliases.get("orders_write")).get("is_write_index"));
        assertTrue(normalizedMappings.containsKey("_doc"));
        assertFalse(((Map<?, ?>) normalizedMappings.get("_doc")).containsKey("_meta"));
    }

    @Test
    void sinkStateShouldTrackSkippedEventDetails() {
        CcEs6SinkState state = new CcEs6SinkState();
        CcEs6TriggerEvent event = new CcEs6TriggerEvent();
        event.setScn(12L);
        event.setEventType(TriggerEventType.UPDATE_ALIASES);

        state.markEventProcessed(11L);
        state.markSkipped(event, "alias incompatible");
        state.markSuccess(12L);

        assertEquals(Long.valueOf(12L), state.getCheckpointScn());
        assertEquals(Long.valueOf(12L), state.getLastProcessedScn());
        assertEquals(Long.valueOf(12L), state.getLastSkippedScn());
        assertEquals("UPDATE_ALIASES", state.getLastSkippedEventType());
        assertEquals("alias incompatible", state.getLastSkippedReason());
    }

    @Test
    void shouldNotApplyGenericRoutingWhenSpecificRoutingExists() throws Exception {
        Map<String, Object> aliasConfig = new HashMap<>();
        aliasConfig.put("routing", "A");
        aliasConfig.put("index_routing", "A");

        Method method = CcEs6ReplayService.class.getDeclaredMethod("buildAddAliasAction", String.class, String.class,
                Map.class);
        method.setAccessible(true);
        IndicesAliasesRequest.AliasActions action = (IndicesAliasesRequest.AliasActions) method.invoke(replayService,
                "orders", "orders_alias", aliasConfig);

        assertEquals("A", action.indexRouting());
        assertEquals(null, action.searchRouting());
    }
}
