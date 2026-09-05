package io.convalesce.emit;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Tests the wrapper an observation travels in. */
public class ObservationTest {

  @Test
  public void payloadIsEmbeddedNotReEncoded() {
    String payload = "{\"deep\":{\"list\":[1,2,3]}}";
    String json = new Observation("spark", "e", payload, "3.5.0", "acme").toJson();
    assertTrue(json.contains(payload));
  }

  @Test
  public void carriesWhatTheReceiverNeedsToRouteIt() {
    String json = new Observation("spark", "SparkListenerJobEnd", "{}", "3.5.0", "acme").toJson();
    assertTrue(json.contains("\"tool\":\"spark\""));
    assertTrue(json.contains("\"event\":\"SparkListenerJobEnd\""));
    assertTrue(json.contains("\"tool_version\":\"3.5.0\""));
    assertTrue(json.contains("\"envelope_version\":1"));
    assertTrue(json.contains("\"client_version\":\"" + Version.VERSION + "\""));
  }

  @Test
  public void everyObservationIsIdentifiable() {
    // A redelivered batch is deduplicated on the id, so two must never share one.
    String first = new Observation("spark", "e", "{}", null, null).toJson();
    String second = new Observation("spark", "e", "{}", null, null).toJson();
    assertNotEquals(first, second);
  }

  @Test
  public void nullPayloadIsTheJsonLiteral() {
    String json = new Observation("spark", "e", null, null, null).toJson();
    assertTrue(json.contains("\"payload\":null"));
  }
}
