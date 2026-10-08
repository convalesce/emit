package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Which variables a setting is read from, and which wins. */
public class ConfigTest {

  @Test
  public void aSettingIsReadUnderThePrefixAPlatformPutsOnIt() {
    Map<String, String> env = new HashMap<String, String>();
    env.put("CUSTOMER_CONVALESCE_ENDPOINT", " https://ingest.example.com ");
    env.put("CUSTOMER_CONVALESCE_INGEST_KEY", "cvl_ingest_abc_secret");
    env.put("CUSTOMER_CONVALESCE_DRY_RUN", "true");
    env.put("CUSTOMER_CONVALESCE_BATCH_SIZE", "7");
    env.put("CUSTOMER_CONVALESCE_SPOOL_MAX_BYTES", "1000");
    Config config = Config.fromEnvironment(env);
    assertEquals("https://ingest.example.com", config.endpoint());
    assertEquals("cvl_ingest_abc_secret", config.ingestKey());
    assertTrue(config.dryRun());
    assertEquals(7, config.batchSize());
    assertEquals(1000L, config.spoolMaxBytes());
  }

  @Test
  public void theSettingsOwnNameWinsOverThePrefixedOne() {
    Map<String, String> env = new HashMap<String, String>();
    env.put("CONVALESCE_ENDPOINT", "https://own.example.com");
    env.put("CUSTOMER_CONVALESCE_ENDPOINT", "https://prefixed.example.com");
    env.put("CONVALESCE_DRY_RUN", "false");
    env.put("CUSTOMER_CONVALESCE_DRY_RUN", "true");
    Config config = Config.fromEnvironment(env);
    assertEquals("https://own.example.com", config.endpoint());
    assertFalse(config.dryRun());
  }

  @Test
  public void withNeitherNameSetTheDefaultsStand() {
    Config config = Config.fromEnvironment(Collections.<String, String>emptyMap());
    assertEquals(Config.DEFAULT_ENDPOINT, config.endpoint());
    assertNull(config.ingestKey());
    assertFalse(config.dryRun());
    assertTrue(config.enabled());
    assertNull(Config.setting(Collections.<String, String>emptyMap(), "CONVALESCE_SPARK_EVENTS"));
  }
}
