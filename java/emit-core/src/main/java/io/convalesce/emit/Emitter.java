package io.convalesce.emit;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends a tool's raw output to Convalesce.
 *
 * <p>Built on {@link HttpURLConnection} rather than {@code java.net.http}: this runs inside someone
 * else's Spark driver, and targeting Java 8 bytecode means the jar loads on every cluster a Spark
 * 3.x job might run on. {@code java.net.http} is Java 11 and up.
 *
 * <p>Nothing here throws into the caller. A customer's job must not fail because our endpoint had a
 * bad minute: we are watching their pipeline, not standing in it.
 */
public final class Emitter {

  private static final Logger LOG = Logger.getLogger(Emitter.class.getName());
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String OBSERVATIONS_PATH = "/v1/observations";

  // Retry only what a retry can fix. A 400 means the receiver understood us and said no; sending
  // it again just spends the job's time.
  private static final Set<Integer> RETRYABLE_STATUS =
      new HashSet<Integer>(Arrays.asList(408, 425, 429, 500, 502, 503, 504));

  // Caps the backoff so a long outage cannot park a driver thread for minutes.
  private static final long MAX_BACKOFF_MS = 30_000L;

  private final Config config;
  private final Random random = new Random();
  private final List<Observation> batch = new ArrayList<Observation>();
  private final Object lock = new Object();
  private final boolean usable;

  /**
   * Builds an emitter.
   *
   * @param config where to send and how hard to try
   */
  public Emitter(Config config) {
    this.config = config;
    String problem = config.validate();
    this.usable = problem == null;
    if (problem != null) {
      LOG.warning("convalesce: not emitting: " + problem);
    }
  }

  /** Builds an emitter from the process environment. */
  public Emitter() {
    this(Config.fromEnvironment());
  }

  /**
   * Queues one observation, sending the batch when it is full.
   *
   * @param tool which tool produced this
   * @param event which callback fired
   * @param payloadJson the tool's own output, already JSON
   * @param toolVersion the tool's version, where it could be read
   */
  public void emit(String tool, String event, String payloadJson, String toolVersion) {
    if (!usable || !config.enabled()) {
      return;
    }
    Observation observation =
        new Observation(tool, event, payloadJson, toolVersion, config.workspace());
    boolean ready;
    synchronized (lock) {
      batch.add(observation);
      ready = batch.size() >= config.batchSize();
    }
    if (ready) {
      flush();
    }
  }

  /** Sends whatever is queued, logging rather than throwing on failure. */
  public void flush() {
    List<Observation> sending;
    synchronized (lock) {
      if (batch.isEmpty()) {
        return;
      }
      sending = new ArrayList<Observation>(batch);
      batch.clear();
    }
    if (config.dryRun()) {
      for (Observation observation : sending) {
        LOG.info("convalesce dry-run: " + observation.toJson());
      }
      return;
    }
    try {
      post(sending);
    } catch (Exception e) {
      // Deliberately broad: see the class docstring. Anything escaping here surfaces inside the
      // customer's job.
      LOG.warning("convalesce: dropped " + sending.size() + " observation(s): " + e.getMessage());
    }
  }

  /** Flushes anything still queued. */
  public void close() {
    flush();
  }

  private void post(List<Observation> sending) throws Exception {
    StringBuilder body = new StringBuilder("{\"observations\":[");
    for (int i = 0; i < sending.size(); i++) {
      if (i > 0) {
        body.append(',');
      }
      body.append(sending.get(i).toJson());
    }
    body.append("]}");
    byte[] encoded = body.toString().getBytes(UTF8);
    String url = trimTrailingSlash(config.endpoint()) + OBSERVATIONS_PATH;

    for (int attempt = 0; attempt <= config.maxRetries(); attempt++) {
      try {
        attempt(url, encoded);
        return;
      } catch (TransportException e) {
        boolean retryable = e.status() == 0 || RETRYABLE_STATUS.contains(e.status());
        if (!retryable || attempt == config.maxRetries()) {
          throw e;
        }
        // Full jitter: several executors failing at once should not come back in lockstep.
        long ceiling = Math.min(1000L << attempt, MAX_BACKOFF_MS);
        Thread.sleep((long) (random.nextDouble() * ceiling));
      }
    }
  }

  private void attempt(String url, byte[] body) throws TransportException {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setConnectTimeout(config.timeoutMs());
      connection.setReadTimeout(config.timeoutMs());
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Authorization", "Bearer " + config.ingestKey());
      connection.setRequestProperty("User-Agent", "convalesce-emit-java/" + Version.VERSION);
      if (config.workspace() != null && !config.workspace().isEmpty()) {
        connection.setRequestProperty("X-Convalesce-Workspace", config.workspace());
      }
      OutputStream out = connection.getOutputStream();
      try {
        out.write(body);
      } finally {
        out.close();
      }
      int status = connection.getResponseCode();
      if (status < 200 || status >= 300) {
        throw new TransportException("HTTP " + status + " from " + url, status);
      }
    } catch (TransportException e) {
      throw e;
    } catch (Exception e) {
      throw new TransportException("could not reach " + url + ": " + e.getMessage(), 0);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static String trimTrailingSlash(String value) {
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  static {
    // Spark's own logging is noisy enough; ours should be visible but not chatty.
    LOG.setLevel(Level.INFO);
  }
}
