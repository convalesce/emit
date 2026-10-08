package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Drives the emitter against a real HTTP server.
 *
 * <p>What leaves the process is the claim this package makes, so a stub at that boundary would test
 * nothing.
 */
public class EmitterTest {

  // Each test's undelivered batches stay its own; a shared spool would be drained into the next
  // test's server.
  @Rule public TemporaryFolder spool = new TemporaryFolder();

  private HttpServer server;
  private String url;
  private final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());
  private final List<Integer> rawLengths = Collections.synchronizedList(new ArrayList<Integer>());
  private final List<String> auth = Collections.synchronizedList(new ArrayList<String>());
  private final List<String> contentEncoding =
      Collections.synchronizedList(new ArrayList<String>());
  private volatile int status = 200;

  @Before
  public void startServer() throws Exception {
    bodies.clear();
    rawLengths.clear();
    auth.clear();
    contentEncoding.clear();
    status = 200;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws java.io.IOException {
            byte[] raw = readBytes(exchange.getRequestBody());
            rawLengths.add(raw.length);
            String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
            contentEncoding.add(String.valueOf(encoding));
            byte[] decoded = "gzip".equals(encoding) ? gunzip(raw) : raw;
            bodies.add(new String(decoded, "UTF-8"));
            auth.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
          }
        });
    server.start();
    url = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  private Emitter emitter(int batchSize, int maxRetries) {
    return new Emitter(
        Config.of(url, "secret-key", batchSize, maxRetries).withSpoolDir(spoolDir()));
  }

  @Test
  public void payloadCrossesVerbatim() {
    // The whole claim: the tool's own JSON is embedded, not re-encoded.
    String payload = "{\"dag\":\"orders\",\"nested\":{\"list\":[1,2],\"null\":null}}";
    emitter(1, 0).emit("spark", "SparkListenerJobEnd", payload, "3.5.0");
    assertEquals(1, bodies.size());
    assertTrue(bodies.get(0).contains(payload));
    assertTrue(bodies.get(0).contains("\"tool\":\"spark\""));
  }

  @Test
  public void bodyIsActuallyGzipCompressedOnTheWire() {
    // Whole-payload forwarding makes a batch bigger than it used to be; gzip is what keeps that
    // from costing the same on the wire. The header alone would not prove it -- the raw bytes
    // must actually be smaller than the decoded text, and must actually be gzip (the handler's own
    // GZIPInputStream already proved that; this repeats the check at the test level).
    String payload = "{\"sql\":\"" + repeat("select * from orders ", 200) + "\"}";
    emitter(1, 0).emit("spark", "e", payload, null);
    assertEquals("gzip", contentEncoding.get(0));
    assertTrue(
        rawLengths.get(0)
            < bodies.get(0).getBytes(java.nio.charset.Charset.forName("UTF-8")).length);
  }

  @Test
  public void keyTravelsInTheHeaderNotTheBody() {
    emitter(1, 0).emit("spark", "e", "{}", null);
    assertEquals("Bearer secret-key", auth.get(0));
    assertFalse(bodies.get(0).contains("secret-key"));
  }

  @Test
  public void fullBatchIsOneRequest() {
    Emitter emitter = emitter(5, 0);
    for (int i = 0; i < 5; i++) {
      emitter.emit("spark", "e", "{\"i\":" + i + "}", null);
    }
    assertEquals(1, bodies.size());
    assertEquals(5, countOccurrences(bodies.get(0), "\"observation_id\""));
  }

  @Test
  public void batchIsClosedByBodySizeNotOnlyCount() {
    // The receiver refuses a body above its limit whole and does not retry, so fifty Spark plan
    // events used to be dropped together.
    Emitter emitter =
        new Emitter(Config.of(url, "secret-key", 50, 0, 10_000).withSpoolDir(spoolDir()));
    String payload = "{\"plan\":\"" + repeat("x", 4000) + "\"}";
    for (int i = 0; i < 6; i++) {
      emitter.emit("spark", "e", payload, null);
    }
    emitter.close();
    assertTrue("expected more than one request, got " + bodies.size(), bodies.size() > 1);
    int total = 0;
    for (String body : bodies) {
      assertTrue("body of " + body.length() + " bytes", body.length() <= 10_000);
      total += countOccurrences(body, "\"observation_id\"");
    }
    assertEquals(6, total);
  }

  @Test
  public void anOversizedObservationGoesAlone() {
    Emitter emitter =
        new Emitter(Config.of(url, "secret-key", 50, 0, 4000).withSpoolDir(spoolDir()));
    emitter.emit("spark", "small", "{\"a\":1}", null);
    emitter.emit("spark", "huge", "{\"plan\":\"" + repeat("x", 8000) + "\"}", null);
    emitter.emit("spark", "small", "{\"b\":2}", null);
    emitter.close();
    boolean alone = false;
    int total = 0;
    for (String body : bodies) {
      int count = countOccurrences(body, "\"observation_id\"");
      total += count;
      if (count == 1 && body.contains("\"huge\"")) {
        alone = true;
      }
    }
    assertTrue("the oversized observation shared a request", alone);
    assertEquals(3, total);
  }

  @Test
  public void fitsSaysWhetherAnObservationWouldGoWithinTheLimit() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 50, 0, 4000));
    assertTrue(emitter.fits("spark", "small", "{\"a\":1}", null));
    assertFalse(emitter.fits("spark", "huge", "{\"plan\":\"" + repeat("x", 8000) + "\"}", null));
  }

  private static String repeat(String unit, int times) {
    StringBuilder out = new StringBuilder(unit.length() * times);
    for (int i = 0; i < times; i++) {
      out.append(unit);
    }
    return out.toString();
  }

  @Test
  public void closeFlushesAPartialBatch() {
    Emitter emitter = emitter(Config.RECEIVER_MAX_OBSERVATIONS, 0);
    emitter.emit("spark", "e", "{}", null);
    assertEquals(0, bodies.size());
    emitter.close();
    assertEquals(1, bodies.size());
  }

  @Test
  public void serverErrorNeverReachesTheCaller() {
    // A customer's job must not fail because our endpoint had a bad minute.
    status = 500;
    emitter(1, 0).emit("spark", "e", "{}", null);
  }

  @Test
  public void unreachableEndpointNeverReachesTheCaller() {
    Emitter emitter =
        new Emitter(Config.of("http://127.0.0.1:1", "k", 1, 0).withSpoolDir(spoolDir()));
    emitter.emit("spark", "e", "{}", null);
  }

  @Test
  public void rejectedRequestIsSentOnce() {
    // A 400 means the receiver understood us and said no.
    status = 400;
    emitter(1, 3).emit("spark", "e", "{}", null);
    assertEquals(1, bodies.size());
  }

  @Test
  public void transientFailureIsRetried() {
    status = 503;
    emitter(1, 2).emit("spark", "e", "{}", null);
    // The first attempt plus two retries.
    assertEquals(3, bodies.size());
  }

  @Test
  public void missingKeyIsReportedNotThrown() {
    assertNotNull(Config.of(url, null, 1, 0).validate());
    new Emitter(Config.of(url, null, 1, 0).withSpoolDir(spoolDir())).emit("spark", "e", "{}", null);
    assertEquals(0, bodies.size());
  }

  @Test
  public void usableConfigValidatesClean() {
    assertNull(Config.of(url, "k", 1, 0).validate());
  }

  @Test
  public void limitsBeyondTheReceiverAreReported() {
    // The receiver refuses such a request whole with a 400, which is not retried.
    assertNotNull(Config.of(url, "k", Config.RECEIVER_MAX_OBSERVATIONS + 1, 0).validate());
    assertNotNull(Config.of(url, "k", 1, 0, Config.RECEIVER_MAX_BODY_BYTES + 1).validate());
    assertNull(Config.of(url, "k", Config.RECEIVER_MAX_OBSERVATIONS, 0).validate());
  }

  private String spoolDir() {
    return spool.getRoot().getPath();
  }

  private int spooled(String kind) {
    String[] names = new java.io.File(spool.getRoot(), kind).list();
    return names == null ? 0 : names.length;
  }

  @Test
  public void undeliveredBatchIsKeptAndSentAfterTheNextSuccess() {
    status = 503;
    Emitter emitter = emitter(1, 0);
    emitter.emit("spark", "lost", "{}", null);
    assertEquals(1, spooled(Spool.PENDING));
    status = 200;
    emitter.emit("spark", "next", "{}", null);
    assertEquals(3, bodies.size());
    assertTrue(bodies.get(2).contains("\"lost\""));
    assertEquals(0, spooled(Spool.PENDING));
  }

  @Test
  public void refusedBatchIsSplitAndEachRefusalKept() {
    status = 400;
    Emitter emitter = emitter(3, 0);
    for (int i = 0; i < 3; i++) {
      emitter.emit("spark", "e" + i, "{}", null);
    }
    // The batch once, then each observation on its own.
    assertEquals(4, bodies.size());
    assertEquals(3, spooled(Spool.REJECTED));
    assertEquals(0, spooled(Spool.PENDING));
  }

  @Test
  public void unreachableEndpointBatchIsKept() {
    new Emitter(Config.of("http://127.0.0.1:1", "k", 1, 0).withSpoolDir(spoolDir()))
        .emit("spark", "e", "{}", null);
    assertEquals(1, spooled(Spool.PENDING));
  }

  @Test
  public void theKeysOwnValueNeverRidesInsideAPayload() {
    // As a job start carries it on AWS Glue, and as a script with the key written in would.
    String key = "key-\"quoted\"-0123456789";
    String payload =
        "{\"Properties\":{\"spark.glue.customer-driver-env-vars\":\"A=1,INGEST=key-\\\"quoted\\\"-0123456789,B=2\"},"
            + "\"note\":\"key-0123456789\"}";
    new Emitter(Config.of(url, key, 1, 0).withSpoolDir(spoolDir()))
        .emit("spark", "SparkListenerJobStart", payload, "3.5.0");
    assertEquals("Bearer " + key, auth.get(0));
    assertFalse(bodies.get(0), bodies.get(0).contains("quoted"));
    assertTrue(bodies.get(0), bodies.get(0).contains("\"A=1,INGEST=***,B=2\""));
    // Something like the key is not the key.
    assertTrue(bodies.get(0), bodies.get(0).contains("\"note\":\"key-0123456789\""));
    assertTrue(
        bodies.get(0),
        bodies.get(0).contains("\"excluded\":[{\"path\":\"$\",\"reason\":\"ingest key masked\"}]"));
  }

  @Test
  public void whatTheCallerMaskedIsDeclaredBesideTheKeyTheEmitterMasked() {
    // A setting whose secret-named entry the caller masked by name, so the key is not found in it.
    String masked = "{\"Properties\":{\"env\":\"A=1,CUSTOMER_CONVALESCE_INGEST_KEY=***\"}}";
    List<Exclusion> declared =
        Collections.singletonList(new Exclusion("Properties.env", Exclusion.KEY_MASKED));
    String key = "key-0123456789";
    Emitter emitter = new Emitter(Config.of(url, key, 1, 0).withSpoolDir(spoolDir()));
    emitter.emit("spark", "SparkListenerJobStart", masked, "3.5.0", declared);
    emitter.emit("spark", "SparkListenerJobStart", "{\"note\":\"" + key + "\"}", "3.5.0", declared);
    String own = "{\"path\":\"Properties.env\",\"reason\":\"ingest key masked\"}";
    assertTrue(bodies.get(0), bodies.get(0).contains("\"excluded\":[" + own + "]"));
    assertTrue(
        bodies.get(1),
        bodies
            .get(1)
            .contains(
                "\"excluded\":[" + own + ",{\"path\":\"$\",\"reason\":\"ingest key masked\"}]"));
  }

  @Test
  public void aPayloadWithoutTheKeyDeclaresNothingAndAShortKeyIsNotLookedFor() {
    String payload = "{\"note\":\"a shortk in ordinary text\"}";
    new Emitter(Config.of(url, "shortk", 1, 0).withSpoolDir(spoolDir()))
        .emit("spark", "e", payload, null);
    emitter(1, 0).emit("spark", "e", payload, null);
    for (String body : bodies) {
      assertTrue(body, body.contains(payload));
      assertTrue(body, body.contains("\"excluded\":[]"));
    }
    assertEquals(2, bodies.size());
  }

  @Test
  public void aDryRunMasksTheKeyAndIsLoggedWhileTheJvmIsUp() {
    java.util.Map<String, String> env = new java.util.HashMap<String, String>();
    env.put("CONVALESCE_INGEST_KEY", "probe-0123456789");
    env.put("CONVALESCE_DRY_RUN", "true");
    env.put("CONVALESCE_FLUSH_INTERVAL", "0");
    env.put("CONVALESCE_SPOOL_DIR", spoolDir());
    final List<String> logged = new ArrayList<String>();
    java.util.logging.Handler handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord record) {
            logged.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    java.util.logging.Logger log = java.util.logging.Logger.getLogger(Emitter.class.getName());
    log.addHandler(handler);
    try {
      Emitter emitter = new Emitter(Config.fromEnvironment(env));
      emitter.emit("spark", "e", "{\"env\":\"INGEST=probe-0123456789\"}", null);
      emitter.flush();
    } finally {
      log.removeHandler(handler);
    }
    assertFalse(Emitter.shuttingDown());
    assertEquals(1, logged.size());
    assertTrue(logged.get(0), logged.get(0).startsWith(Emitter.DRY_RUN_PREFIX));
    assertTrue(logged.get(0), logged.get(0).contains("INGEST=***"));
    assertFalse(logged.get(0), logged.get(0).contains("0123456789"));
    assertEquals(0, bodies.size());
  }

  @Test
  public void aDryRunPrintsWhatIsFlushedAsTheJvmShutsDown() throws Exception {
    // Only a JVM that is shutting down can show it, so one is started and left to end with an
    // observation still queued, as a driver that fails without stopping its session does.
    String launcher = System.getProperty("java.home") + "/bin/java";
    ProcessBuilder builder =
        new ProcessBuilder(
            launcher, "-cp", System.getProperty("java.class.path"), QueuedAtExit.class.getName());
    builder.environment().put("CONVALESCE_INGEST_KEY", "probe-0123456789");
    builder.environment().put("CONVALESCE_DRY_RUN", "true");
    builder.environment().put("CONVALESCE_SPOOL_DIR", spoolDir());
    builder.redirectErrorStream(true);
    Process process = builder.start();
    String printed = new String(readBytes(process.getInputStream()), "UTF-8");
    assertEquals(printed, 0, process.waitFor());
    assertEquals(printed, 1, countOccurrences(printed, Emitter.DRY_RUN_PREFIX));
    assertTrue(printed, printed.contains("\"event\":\"SparkListenerApplicationEnd\""));
    assertTrue(printed, printed.contains("\"last\":true"));
  }

  /** A driver that ends with its last observation still queued. */
  public static final class QueuedAtExit {
    private QueuedAtExit() {}

    /**
     * Queues one observation and returns, leaving the shutdown hook to flush it.
     *
     * @param args unused
     */
    public static void main(String[] args) {
      // Logging that is already gone by the time the hook runs, as a driver's is.
      java.util.logging.LogManager.getLogManager().reset();
      new Emitter().emit("spark", "SparkListenerApplicationEnd", "{\"last\":true}", "3.5.3");
    }
  }

  private static byte[] readBytes(InputStream in) throws java.io.IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(byte[] data) throws java.io.IOException {
    GZIPInputStream unzipped = new GZIPInputStream(new java.io.ByteArrayInputStream(data));
    try {
      return readBytes(unzipped);
    } finally {
      unzipped.close();
    }
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int at = haystack.indexOf(needle);
    while (at != -1) {
      count++;
      at = haystack.indexOf(needle, at + needle.length());
    }
    return count;
  }
}
