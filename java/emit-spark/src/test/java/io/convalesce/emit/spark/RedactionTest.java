package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.junit.Test;
import scala.collection.JavaConverters;

/** Credentials in an event's configuration never leave the driver. */
public class RedactionTest {

  private static final Redaction DEFAULT = Redaction.of(null);

  @Test
  public void aRealJobStartIsSentWithoutTheSecretsItsConfigurationHeld() {
    Properties properties = new Properties();
    properties.setProperty("spark.app.id", "local-1789");
    properties.setProperty("spark.hadoop.fs.s3a.secret.key", "hunter2");
    properties.setProperty("spark.jdbc.url", "jdbc:postgresql://db/shop?password=hunter2");
    properties.setProperty("spark.master", "local[2]");
    SparkListenerJobStart event =
        new SparkListenerJobStart(
            0,
            1L,
            JavaConverters.asScalaBufferConverter(new ArrayList<StageInfo>()).asScala().toSeq(),
            properties);
    // Spark's own serialiser does not redact: only its event log does.
    assertTrue(SparkEventJson.toJson(event).contains("hunter2"));

    String sent = DEFAULT.apply(SparkEventJson.toJson(event));
    assertFalse(sent, sent.contains("hunter2"));
    assertTrue(sent.contains("\"spark.master\":\"local[2]\""));
    assertTrue(
        sent.contains("\"spark.hadoop.fs.s3a.secret.key\":\"" + Redaction.REPLACEMENT + "\""));
    assertEquals("local-1789", SparkEventJson.readAppId(sent));
  }

  @Test
  public void aValueNamingACredentialGoesEvenUnderAnInnocentKey() {
    String json = "{\"modifiedConfigs\":{\"spark.url\":\"jdbc:x?password=p\",\"a\":\"b\"}}";
    assertEquals(
        "{\"modifiedConfigs\":{\"spark.url\":\"" + Redaction.REPLACEMENT + "\",\"a\":\"b\"}}",
        DEFAULT.apply(json));
  }

  @Test
  public void onlyConfigurationIsRedacted() {
    // A description can say "token" without holding one.
    String json =
        "{\"description\":\"tokenize at <unknown>:0\",\"Properties\":{\"spark.app.id\":\"x\"}}";
    assertEquals(json, DEFAULT.apply(json));
  }

  @Test
  public void everyConfigurationMapInTheEventIsRedacted() {
    String json =
        "{\"Properties\" : {\"my.token\" : \"t\"},"
            + "\"Stage Infos\":[{\"Properties\":{\"access.key\":\"k\"}}],"
            + "\"Spark Properties\":{\"x.password\":\"p\"}}";
    String sent = DEFAULT.apply(json);
    assertFalse(sent, sent.contains("\"t\""));
    assertFalse(sent, sent.contains("\"k\""));
    assertFalse(sent, sent.contains("\"p\""));
  }

  @Test
  public void aMapThatIsNotFlatStringsIsLeftAsItIs() {
    String json = "{\"Properties\":{\"a\":1,\"password\":\"p\"}}";
    assertEquals(json, DEFAULT.apply(json));
    String empty = "{\"Properties\":{},\"modifiedConfigs\" :{ }}";
    assertEquals(empty, DEFAULT.apply(empty));
  }

  @Test
  public void escapedQuotesDoNotEndAValue() {
    String json = "{\"Properties\":{\"a\":\"say \\\"hi\\\"\",\"secret\":\"s\\\"x\"}}";
    assertEquals(
        "{\"Properties\":{\"a\":\"say \\\"hi\\\"\",\"secret\":\"" + Redaction.REPLACEMENT + "\"}}",
        DEFAULT.apply(json));
  }

  @Test
  public void theJobsOwnRuleIsUsedAndABrokenOneFallsBackToSparks() {
    String json = "{\"Properties\":{\"my.pin\":\"1234\",\"password\":\"p\"}}";
    assertEquals(
        "{\"Properties\":{\"my.pin\":\"" + Redaction.REPLACEMENT + "\",\"password\":\"p\"}}",
        Redaction.of("pin").apply(json));
    assertFalse(Redaction.of("(unclosed").apply(json).contains("\"p\""));
    assertEquals(null, DEFAULT.apply(null));
  }
}
