package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.convalesce.emit.Exclusion;
import java.util.ArrayList;
import java.util.List;
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
  public void ourOwnKeyIsASecretWhateverRuleTheJobConfigured() {
    // A job's own rule replaces Spark's default and knows nothing of our setting's name.
    String json =
        "{\"Properties\":{\"spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY\":\"abc123\","
            + "\"spark.executorEnv.CUSTOMER_CONVALESCE_INGEST_KEY\":\"abc123\","
            + "\"spark.convalesce.ingest.key\":\"abc123\",\"spark.convalesce.ingestKey\":\"abc123\","
            + "\"spark.master\":\"yarn\"}}";
    for (String regex : new String[] {null, "(?i)passphrase", "("}) {
      String sent = Redaction.of(regex).apply(json);
      assertFalse(sent, sent.contains("abc123"));
      assertTrue(sent, sent.contains("\"spark.master\":\"yarn\""));
      assertTrue(
          sent,
          sent.contains(
              "\"spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY\":\""
                  + Redaction.REPLACEMENT
                  + "\""));
    }
  }

  @Test
  public void aSecretNamedEntryInsideAValueIsMaskedAndTheRestKept() {
    // The parameter AWS Glue hands a job its environment in, as every job start carries it.
    String json =
        "{\"Properties\":{\"spark.glue.customer-driver-env-vars\":"
            + "\"CUSTOMER_CONVALESCE_ENDPOINT=https://example.invalid/openapi,"
            + "CUSTOMER_CONVALESCE_INGEST_KEY=abc123,CUSTOMER_CONVALESCE_DRY_RUN=true\","
            + "\"spark.app.id\":\"x\"}}";
    assertEquals(
        "{\"Properties\":{\"spark.glue.customer-driver-env-vars\":"
            + "\"CUSTOMER_CONVALESCE_ENDPOINT=https://example.invalid/openapi,"
            + "CUSTOMER_CONVALESCE_INGEST_KEY=***,CUSTOMER_CONVALESCE_DRY_RUN=true\","
            + "\"spark.app.id\":\"x\"}}",
        DEFAULT.apply(json));
  }

  @Test
  public void anEntryMaskedInsideAValueIsDeclaredByItsSettingOnce() {
    // A value replaced whole says so itself; one with an entry masked inside it does not.
    String json =
        "{\"Properties\":{\"spark.glue.customer-driver-env-vars\":"
            + "\"A=1,CUSTOMER_CONVALESCE_INGEST_KEY=abc123,B=2\","
            + "\"spark.hadoop.fs.s3a.secret.key\":\"hunter2\",\"spark.sql.a\":\"x=y\"},"
            + "\"Stage Infos\":[{\"Properties\":{\"spark.glue.customer-driver-env-vars\":"
            + "\"CUSTOMER_CONVALESCE_INGEST_KEY=abc123\"}}]}";
    List<Exclusion> masked = new ArrayList<Exclusion>();
    String sent = DEFAULT.apply(json, masked);
    assertFalse(sent, sent.contains("abc123"));
    assertEquals(1, masked.size());
    assertEquals("Properties.spark.glue.customer-driver-env-vars", masked.get(0).path());
    assertEquals("ingest key masked", masked.get(0).reason());
  }

  @Test
  public void anEntryMaskedInARunEventIsDeclaredUnderTheMapsItIsIn() {
    String json =
        "{\"run\":{\"facets\":{\"environment-properties\":{\"environment-properties\":{"
            + "\"mounts\":{\"env\":\"A=1,CONVALESCE_INGEST_KEY=abc123\"}}}}}}";
    List<Exclusion> masked = new ArrayList<Exclusion>();
    String sent = DEFAULT.applyToRunEvent(json, masked);
    assertFalse(sent, sent.contains("abc123"));
    assertEquals(1, masked.size());
    // The facet, the map in it, and the map in that.
    assertEquals("environment-properties.environment-properties.mounts.env", masked.get(0).path());
  }

  @Test
  public void anEntryNamedByTheJobsOwnRuleIsMaskedToo() {
    // Entries apart from one another by a space, a semicolon or an escaped line end.
    String json =
        "{\"System Properties\":{\"sun.java.command\":\"submit --conf "
            + "spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY=abc123 --conf a=b\","
            + "\"env\":\"REGION=north;DB_PASSPHRASE=abc123\\nMODE=fast\"}}";
    Redaction rule = Redaction.of("(?i)passphrase");
    // The rule's own word in a value takes the whole value, as Spark has it; a name only our rule
    // knows takes its entry.
    assertEquals(
        "{\"System Properties\":{\"sun.java.command\":\"submit --conf "
            + "spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY=*** --conf a=b\","
            + "\"env\":\""
            + Redaction.REPLACEMENT
            + "\"}}",
        rule.apply(json));
  }

  @Test
  public void aValueWithNoSecretNamedEntryIsLeftAsItIs() {
    String json =
        "{\"Properties\":{\"spark.executor.extraJavaOptions\":\"-Dkey=1 -Dingest=2\","
            + "\"spark.sql.a\":\"x=y\"}}";
    assertEquals(json, DEFAULT.apply(json));
  }

  @Test
  public void ourOwnKeyIsMaskedInAnOpenLineageRunEventToo() {
    String json =
        "{\"run\":{\"facets\":{\"spark_properties\":{\"properties\":{"
            + "\"spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY\":\"abc123\","
            + "\"spark.glue.customer-driver-env-vars\":\"A=1,CUSTOMER_CONVALESCE_INGEST_KEY=abc123\"}}}}}";
    String sent = Redaction.of("(?i)passphrase").applyToRunEvent(json);
    assertFalse(sent, sent.contains("abc123"));
    assertTrue(sent, sent.contains("A=1,CUSTOMER_CONVALESCE_INGEST_KEY=***"));
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

  @Test
  public void anOpenLineageRunEventLosesTheSecretsItsFacetsCaptured() {
    String json =
        "{\"run\":{\"facets\":{\"spark_properties\":{\"_producer\":\"p\",\"properties\":{"
            + "\"spark.master\":\"local\",\"spark.hadoop.fs.s3a.secret.key\":\"hunter2\","
            + "\"spark.jdbc.url\":\"jdbc:postgresql://db/shop?password=hunter2\"}},"
            + "\"environment-properties\":{\"_producer\":\"p\",\"environment-properties\":{"
            + "\"mountPoints\":[{\"mountPoint\":\"/mnt\",\"source\":\"s3a://b\"}],"
            + "\"cores\":8,\"api.token\":{\"value\":\"hunter2\"},\"cluster\":\"etl\","
            + "\"tags\":{\"owner\":\"etl\",\"db.password\":\"hunter2\"}}}}},"
            + "\"job\":{\"name\":\"token_refresh\"}}";
    String sent = DEFAULT.applyToRunEvent(json);
    assertFalse(sent, sent.contains("hunter2"));
    assertTrue(sent, sent.contains("\"spark.master\":\"local\""));
    assertTrue(
        sent,
        sent.contains("\"spark.hadoop.fs.s3a.secret.key\":\"" + Redaction.REPLACEMENT + "\""));
    // What is not a string is kept as it was, unless its key names a credential.
    assertTrue(sent, sent.contains("\"mountPoints\":[{\"mountPoint\":\"/mnt\""));
    assertTrue(sent, sent.contains("\"cores\":8,\"api.token\":\"" + Redaction.REPLACEMENT));
    assertTrue(sent, sent.contains("\"cluster\":\"etl\""));
    assertTrue(
        sent,
        sent.contains(
            "\"tags\":{\"owner\":\"etl\",\"db.password\":\"" + Redaction.REPLACEMENT + "\"}"));
    // A job's name can say "token" without holding one.
    assertTrue(sent, sent.contains("\"name\":\"token_refresh\""));
  }

  @Test
  public void aRunEventWithNothingToHideIsSentAsItWas() {
    String json =
        "{\"run\":{\"facets\":{\"spark_properties\":{\"properties\":{\"spark.master\":\"local\"}}}},"
            + "\"inputs\":[{\"name\":\"shop.orders\"}]}";
    assertEquals(json, DEFAULT.applyToRunEvent(json));
    assertEquals(null, DEFAULT.applyToRunEvent(null));
    // Cut short, it is left alone rather than guessed at.
    String cut = "{\"properties\":{\"password\":\"p\",\"a\":[1,";
    assertEquals(cut, DEFAULT.applyToRunEvent(cut));
  }
}
