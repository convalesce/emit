package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
import org.apache.spark.ExecutorLostFailure;
import org.apache.spark.SparkException;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;

/**
 * A failed job's last lines leave with its end, and no credential among them leaves as it is.
 *
 * <p>Read off a real HTTP server, as the receiver would: what is asserted is what crossed.
 */
public class SparkOutputTest {

  private static final String END = "{\"Event\":\"SparkListenerJobEnd\",\"Job ID\":7}";
  private static final Map<String, String> ON = Collections.<String, String>emptyMap();

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
  public void aFailedJobCarriesWhatItFailedWith() {
    listener(ON).onJobEnd(failed("Job aborted: cannot resolve 'sales_amount'"));

    assertEquals(1, bodies.size());
    String sent = bodies.get(0);
    assertTrue(sent, sent.contains("\"event\":\"SparkListenerJobEnd\""));
    assertTrue(
        sent, sent.contains("\"output_tail\":\"Job aborted: cannot resolve 'sales_amount'\""));
    assertTrue(sent, sent.contains("\"excluded\":[]"));
  }

  @Test
  public void aJobThatSucceedsCarriesNone() {
    ConvalesceSparkListener listener = listener(ON);
    listener.onTaskEnd(failedTask("a task that was retried"));
    listener.onJobEnd(new SparkListenerJobEnd(7, 99L, JobSucceeded$.MODULE$));

    String sent = bodies.get(bodies.size() - 1);
    assertTrue(sent, sent.contains("\"event\":\"SparkListenerJobEnd\""));
    assertFalse(sent, sent.contains("output_tail"));
    assertFalse(sent, sent.contains("a task that was retried"));
  }

  @Test
  public void theTasksErrorComesBeforeTheJobsAndOneJobsIsNotTheNexts() {
    ConvalesceSparkListener listener = listener(ON);
    listener.onTaskEnd(failedTask("bad row 17"));
    listener.onJobEnd(failed("Job aborted due to stage failure"));
    listener.onJobEnd(failed("Another job, another reason"));

    String first = bodies.get(bodies.size() - 2);
    assertTrue(first, first.contains("\"output_tail\":\"ExecutorLostFailure (executor 1 "));
    assertTrue(first, first.contains("Reason: bad row 17\\nJob aborted due to stage failure\""));
    String second = bodies.get(bodies.size() - 1);
    assertTrue(second, second.contains("\"output_tail\":\"Another job, another reason\""));
  }

  @Test
  public void noSeededCredentialCrossesAndTheMaskingIsDeclared() {
    listener(ON)
        .onJobEnd(
            failed(
                "Job aborted: could not connect to jdbc:postgresql://svc:hunter2@db.internal/app\n"
                    + "with password=hunter2\n"
                    + "-----BEGIN PRIVATE KEY-----\n"
                    + "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
                    + "-----END PRIVATE KEY-----"));

    // Spark's own rendering of the job's result, beside the tail, is what Spark wrote and is not
    // this field's to change; what is asserted is the tail.
    String sent = bodies.get(0);
    String tail = sent.substring(sent.indexOf("\"output_tail\":"));
    assertFalse(tail, tail.contains("hunter2"));
    assertFalse(tail, tail.contains("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC"));
    assertTrue(tail, tail.contains("svc:***@db.internal"));
    assertTrue(
        sent,
        sent.contains(
            "\"excluded\":[{\"path\":\"output_tail\",\"reason\":\"credential masked\"}]"));
  }

  @Test
  public void switchedOffAFailedJobIsWhatSparkRendered() {
    for (String name : new String[] {"CONVALESCE_SEND_OUTPUT", "CUSTOMER_CONVALESCE_SEND_OUTPUT"}) {
      bodies.clear();
      Map<String, String> off = new HashMap<String, String>();
      off.put(name, "false");
      listener(off).onJobEnd(failed("Job aborted: cannot resolve 'sales_amount'"));

      assertEquals(1, bodies.size());
      assertFalse(bodies.get(0), bodies.get(0).contains("output_tail"));
      List<Exclusion> excluded = new ArrayList<Exclusion>();
      assertEquals(END, ConvalesceSparkListener.withOutput(END, off, "it failed", excluded));
      assertTrue(excluded.isEmpty());
    }
  }

  @Test
  public void theEndIsKeptWithinBothLimitsAndTheCutIsDeclared() {
    StringBuilder reason = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      reason.append(String.format("line %04d ", i));
      for (int j = 0; j < 150; j++) {
        reason.append('x');
      }
      reason.append('\n');
    }
    List<Exclusion> excluded = new ArrayList<Exclusion>();
    String json = ConvalesceSparkListener.withOutput(END, ON, reason.toString(), excluded);
    String tail = json.substring(json.indexOf("\"output_tail\":\"") + 15, json.length() - 2);
    assertTrue(tail.split("\\\\n").length <= 60);
    assertTrue(tail.replace("\\n", "\n").length() <= 4096);
    assertTrue(tail, tail.contains("line 0499 "));
    assertFalse(tail, tail.contains("line 0400 "));
    assertEquals(2, excluded.size());
    assertEquals("limited to 60 lines", excluded.get(0).reason());
    assertEquals("limited to 4096 bytes", excluded.get(1).reason());
  }

  @Test
  public void ofEachStackOnlyTheFramesNearestItsErrorAreKept() {
    String trace =
        "java.lang.RuntimeException: boom\n"
            + "\tat a.A.one(A.java:1)\n"
            + "\tat a.A.two(A.java:2)\n"
            + "\tat a.A.three(A.java:3)\n"
            + "\tat a.A.four(A.java:4)\n"
            + "\t... 12 more\n"
            + "Caused by: java.io.IOException: disk\n"
            + "\tat b.B.one(B.java:1)\n"
            + "\n"
            + "Driver stacktrace:";
    assertEquals(
        "java.lang.RuntimeException: boom\n"
            + "\tat a.A.one(A.java:1)\n"
            + "\tat a.A.two(A.java:2)\n"
            + "\tat a.A.three(A.java:3)\n"
            + "Caused by: java.io.IOException: disk\n"
            + "\tat b.B.one(B.java:1)\n"
            + "Driver stacktrace:",
        ConvalesceSparkListener.failureText(null, null, trace));
  }

  @Test
  public void whatALaterReasonQuotesWholeIsSaidOnce() {
    String task = "java.lang.RuntimeException: boom";
    String stage = "Task 0 in stage 1.0 failed 1 times: " + task;
    String job = "Job aborted due to stage failure: " + stage;
    assertEquals(job, ConvalesceSparkListener.failureText(task, stage, job));
    assertEquals(
        "another error\n" + job, ConvalesceSparkListener.failureText("another error", stage, job));
    assertEquals(job, ConvalesceSparkListener.failureText(" ", null, job));
  }

  @Test
  public void aJobThatDidNotFailHasNoReason() {
    assertNull(
        ConvalesceSparkListener.reasonOf(new SparkListenerJobEnd(7, 99L, JobSucceeded$.MODULE$)));
    assertEquals("why", ConvalesceSparkListener.reasonOf(failed("why")));
  }

  private static SparkListenerJobEnd failed(String reason) {
    return new SparkListenerJobEnd(7, 99L, new JobFailed(new SparkException(reason)));
  }

  // The one failure Spark lets a test build with words of its own in it.
  private static SparkListenerTaskEnd failedTask(String why) {
    ExecutorLostFailure reason = new ExecutorLostFailure("1", true, Option.apply(why));
    return new SparkListenerTaskEnd(1, 0, "ResultTask", reason, null, null, null);
  }

  private ConvalesceSparkListener listener(Map<String, String> env) {
    Config config =
        Config.of("http://127.0.0.1:" + server.getAddress().getPort(), "ingest-key", 1, 0)
            .withSpoolDir(spool.getRoot().getPath());
    return new ConvalesceSparkListener(new Emitter(config), "3.5.0", Redaction.of(null), null, env);
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
