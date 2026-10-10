package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.convalesce.emit.Config;
import io.convalesce.emit.Emitter;
import io.convalesce.emit.Exclusion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * The driver's settings leave with the application's end, and no secret among them leaves as it is.
 *
 * <p>Read off a real HTTP server, as the receiver would: what is asserted is what crossed.
 */
public class SparkSettingsTest {

  private static final String END = "{\"Event\":\"SparkListenerApplicationEnd\",\"Timestamp\":1}";
  // The rows of testdata/settings_vectors.json for the key `ingest`.
  private static final String HASHED =
      "{\"kind\":\"environment\",\"name\":\"DB_PASSWORD\",\"fingerprint\":\"7ee700b0447bb731\"}";
  private static final String KEYED_BY = "\"keyed_by\":\"2b4957b70d863694\"";

  @Rule public TemporaryFolder spool = new TemporaryFolder();

  private HttpServer server;
  private final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());

  @Before
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            byte[] raw = read(exchange.getRequestBody());
            boolean gzip = "gzip".equals(exchange.getRequestHeaders().getFirst("Content-Encoding"));
            bodies.add(new String(gzip ? gunzip(raw) : raw, "UTF-8"));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
          }
        });
    server.start();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  @Test
  public void anApplicationEndCarriesTheDriversEnvironmentWithEachSecretHashed() {
    Map<String, String> env = on();
    env.put("CONVALESCE_INGEST_KEY", "ingest");
    env.put("DB_PASSWORD", "hunter2");
    env.put("JDBC_URL", "jdbc:postgresql://shop:hunter3@db/shop");
    env.put("TZ", "Europe/London");
    listener(env).onApplicationEnd(new SparkListenerApplicationEnd(1L));

    assertEquals(1, bodies.size());
    String sent = bodies.get(0);
    assertTrue(sent, sent.contains("\"event\":\"SparkListenerApplicationEnd\""));
    assertTrue(sent, sent.contains("\"settings\":{\"items\":["));
    assertTrue(sent, sent.contains(HASHED));
    assertTrue(sent, sent.contains("\"name\":\"JDBC_URL\",\"fingerprint\":\""));
    assertTrue(
        sent,
        sent.contains("{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"Europe/London\"}"));
    assertTrue(sent, sent.contains(KEYED_BY));
    assertFalse(sent, sent.contains("hunter"));
    assertFalse(sent, sent.contains("CONVALESCE_INGEST_KEY"));
    assertTrue(sent, sent.contains("\"excluded\":[]"));
  }

  @Test
  public void withTheSettingOffAnApplicationEndIsWhatSparkRendered() {
    Map<String, String> env = new HashMap<String, String>();
    env.put("CONVALESCE_INGEST_KEY", "ingest");
    env.put("TZ", "Europe/London");
    listener(env).onApplicationEnd(new SparkListenerApplicationEnd(1L));

    assertEquals(1, bodies.size());
    assertFalse(bodies.get(0), bodies.get(0).contains("settings"));
    assertFalse(bodies.get(0), bodies.get(0).contains("Europe/London"));
    assertEquals(END, ConvalesceSparkListener.withSettings(END, env, new ArrayList<Exclusion>()));
  }

  @Test
  public void onlyTheApplicationsEndCarriesThem() {
    Map<String, String> env = on();
    env.put("TZ", "Europe/London");
    listener(env).onJobEnd(new SparkListenerJobEnd(7, 99L, JobSucceeded$.MODULE$));

    assertEquals(1, bodies.size());
    assertTrue(bodies.get(0), bodies.get(0).contains("\"event\":\"SparkListenerJobEnd\""));
    assertFalse(bodies.get(0), bodies.get(0).contains("settings"));
  }

  @Test
  public void whatIsLeftOutIsDeclaredOnTheObservation() {
    // No ingest key in the driver's environment and no key of the customer's own: nothing to hash
    // with.
    Map<String, String> env = on();
    env.put("DB_PASSWORD", "hunter2");
    for (int i = 0; i < 501; i++) {
      env.put(String.format("N%04d", i), "v");
    }
    listener(env).onApplicationEnd(new SparkListenerApplicationEnd(1L));

    String sent = bodies.get(0);
    assertTrue(
        sent,
        sent.contains(
            "\"excluded\":["
                + "{\"path\":\"settings.environment\",\"reason\":\"limited to 500 names\"},"
                + "{\"path\":\"settings\",\"reason\":\"no key to fingerprint 1 settings with\"}]"));
    assertFalse(sent, sent.contains("hunter2"));
    assertFalse(sent, sent.contains("fingerprint\":"));
    assertFalse(sent, sent.contains("keyed_by"));
  }

  @Test
  public void theFieldIsAddedLastAndTheEventIsOtherwiseAsItWas() {
    Map<String, String> env = on();
    env.put("CUSTOMER_CONVALESCE_INGEST_KEY", " ingest ");
    env.put("DB_PASSWORD", "hunter2");
    List<Exclusion> excluded = new ArrayList<Exclusion>();
    assertEquals(
        "{\"Event\":\"SparkListenerApplicationEnd\",\"Timestamp\":1,\"settings\":{\"items\":["
            + "{\"kind\":\"environment\",\"name\":\"CONVALESCE_SEND_SETTINGS\",\"value\":\"true\"},"
            + HASHED
            + "],"
            + KEYED_BY
            + "}}",
        ConvalesceSparkListener.withSettings(END, env, excluded));
    assertTrue(excluded.isEmpty());
    assertEquals("{\"settings\":{}}", SparkEventJson.withField("{}", "settings", "{}"));
    assertEquals("[1]", SparkEventJson.withField("[1]", "settings", "{}"));
    assertEquals(END, SparkEventJson.withField(END, "settings", null));
  }

  @Test
  public void sparksRedactionCannotReachIntoTheSettings() {
    // Names and values chosen to read like everything the redaction looks for: a name its rule
    // matches, a name of one of the maps it rewrites, and a value that spells such a map out.
    Map<String, String> env = on();
    env.put("CONVALESCE_INGEST_KEY", "ingest");
    env.put("DB_PASSWORD", "hunter2");
    env.put("Properties", "x");
    env.put("NOTE", "\"Properties\":{\"spark.password\":\"x\",\"a\":\"TOKEN=abc\"}");
    env.put("OPTIONS", "-Dfoo=1 -Dingest_key=2");
    String payload = ConvalesceSparkListener.withSettings(END, env, new ArrayList<Exclusion>());
    assertTrue(payload, payload.contains(HASHED));
    assertTrue(payload, payload.contains(KEYED_BY));
    assertFalse(payload, payload.contains("hunter2"));

    // The listener adds the field after redacting, so the redaction never sees it. Were that order
    // ever turned round, it would still find nothing: it rewrites only the maps Spark names, and
    // `settings` is not one, nor can a value quoted inside it open one.
    List<Exclusion> masked = new ArrayList<Exclusion>();
    assertEquals(payload, Redaction.of(null).apply(payload, masked));
    assertEquals(
        payload,
        Redaction.of("(?i)password|keyed|fingerprint|environment|settings|items|name")
            .apply(payload, masked));
    assertEquals(payload, Redaction.of(null).applyToRunEvent(payload, masked));
    assertTrue(masked.isEmpty());

    // And a real configuration map beside it is still redacted, the settings still untouched.
    String both = "{\"Properties\":{\"spark.db.password\":\"hunter2\"}," + payload.substring(1);
    String redacted = Redaction.of(null).apply(both);
    assertFalse(redacted, redacted.contains("hunter2"));
    assertTrue(redacted, redacted.endsWith(payload.substring(1)));
  }

  private ConvalesceSparkListener listener(Map<String, String> env) {
    // The emitter's own key is what authorises the send; the environment's is what a hash is keyed
    // with, and in a real driver they are the same variable.
    Config config =
        Config.of("http://127.0.0.1:" + server.getAddress().getPort(), "ingest-key", 1, 0)
            .withSpoolDir(spool.getRoot().getPath());
    return new ConvalesceSparkListener(new Emitter(config), "3.5.0", Redaction.of(null), null, env);
  }

  private static Map<String, String> on() {
    Map<String, String> env = new HashMap<String, String>();
    env.put("CONVALESCE_SEND_SETTINGS", "true");
    return env;
  }

  private static byte[] read(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    for (int n = in.read(buffer); n > 0; n = in.read(buffer)) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(byte[] raw) throws IOException {
    return read(new GZIPInputStream(new java.io.ByteArrayInputStream(raw)));
  }
}
