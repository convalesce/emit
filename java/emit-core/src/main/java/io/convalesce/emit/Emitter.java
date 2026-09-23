package io.convalesce.emit;

import java.io.File;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
 *
 * <p>Nothing here is thrown away either. A batch that cannot be delivered goes to the local {@link
 * Spool} and is sent again after the next send that succeeds; a refused batch is split so one bad
 * observation cannot take the others with it, and whatever is still refused is kept on disk. A part
 * batch is flushed in the background every few seconds and when the JVM shuts down, so a driver
 * that dies between events loses nothing it had already queued.
 */
public final class Emitter {

  private static final Logger LOG = Logger.getLogger(Emitter.class.getName());
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String OBSERVATIONS_PATH = "/v1/observations";
  private static final String BODY_OPEN = "{\"observations\":[";
  private static final String BODY_CLOSE = "]}";

  // Retry only what a retry can fix. A 400 means the receiver understood us and said no; sending
  // it again just spends the job's time.
  private static final Set<Integer> RETRYABLE_STATUS =
      new HashSet<Integer>(Arrays.asList(408, 425, 429, 500, 502, 503, 504));

  // The receiver read the batch and will never take it as sent. Anything else that fails is worth
  // sending again later.
  private static final Set<Integer> REFUSED_STATUS =
      new HashSet<Integer>(Arrays.asList(400, 413, 422));

  // Spooled batches sent after one successful send, at most, so a long outage's backlog cannot
  // hold up the send that finally got through.
  private static final int DRAIN_PER_SEND = 20;

  // Caps the backoff so a long outage cannot park a driver thread for minutes.
  private static final long MAX_BACKOFF_MS = 30_000L;

  private final Config config;
  private final Random random = new Random();
  private final List<Observation> batch = new ArrayList<Observation>();
  private final List<byte[]> encoded = new ArrayList<byte[]>();
  private int batchBytes = 0;
  private final Object lock = new Object();
  private final boolean usable;
  private final Spool spool;

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
    this.spool = new Spool(config.spoolDir(), config.spoolMaxBytes());
    if (usable && config.enabled()) {
      startBackgroundFlush();
    }
  }

  private void startBackgroundFlush() {
    Runnable flusher =
        new Runnable() {
          @Override
          public void run() {
            try {
              flush();
            } catch (Throwable t) {
              LOG.warning("convalesce: background flush failed: " + t.getMessage());
            }
          }
        };
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(flusher, "convalesce-emit-shutdown"));
    } catch (Exception e) {
      // Already shutting down; there is nothing left to hook.
      LOG.fine("convalesce: no shutdown flush: " + e.getMessage());
    }
    int interval = config.flushIntervalMs();
    if (interval <= 0) {
      return;
    }
    ScheduledExecutorService timer =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable task) {
                Thread thread = new Thread(task, "convalesce-emit-flush");
                // Never the reason a driver stays up after its job is done.
                thread.setDaemon(true);
                return thread;
              }
            });
    timer.scheduleWithFixedDelay(flusher, interval, interval, TimeUnit.MILLISECONDS);
  }

  /** Builds an emitter from the process environment. */
  public Emitter() {
    this(Config.fromEnvironment());
  }

  /**
   * Queues one observation, sending the batch when it is full.
   *
   * <p>Full by count or by size: the receiver refuses a body above its limit whole, so an
   * observation that would push the batch past it goes into the next batch, and one that is over
   * the limit on its own is sent on its own, so at worst one is refused rather than fifty.
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
    Observation observation = new Observation(tool, event, payloadJson, toolVersion);
    byte[] bytes = observation.toJson().getBytes(UTF8);
    int overhead = BODY_OPEN.length() + BODY_CLOSE.length();
    if (bytes.length + overhead > config.maxBodyBytes()) {
      LOG.warning(
          "convalesce: "
              + tool
              + "/"
              + event
              + " is "
              + bytes.length
              + " bytes, above the receiver's limit of "
              + config.maxBodyBytes()
              + "; sending it alone and it may be refused");
    }
    List<byte[]> closed = null;
    boolean ready;
    synchronized (lock) {
      // A comma per observation joins them in the body.
      int projected = batchBytes + bytes.length + encoded.size() + overhead;
      if (!encoded.isEmpty() && projected > config.maxBodyBytes()) {
        closed = new ArrayList<byte[]>(encoded);
        encoded.clear();
        batch.clear();
        batchBytes = 0;
      }
      batch.add(observation);
      encoded.add(bytes);
      batchBytes += bytes.length;
      ready = batch.size() >= config.batchSize();
    }
    if (closed != null) {
      send(closed);
    }
    if (ready) {
      flush();
    }
  }

  /** Sends whatever is queued, logging rather than throwing on failure. */
  public void flush() {
    List<byte[]> sending;
    synchronized (lock) {
      if (encoded.isEmpty()) {
        return;
      }
      sending = new ArrayList<byte[]>(encoded);
      encoded.clear();
      batch.clear();
      batchBytes = 0;
    }
    send(sending);
  }

  private void send(List<byte[]> sending) {
    if (config.dryRun()) {
      for (byte[] observation : sending) {
        LOG.info("convalesce dry-run: " + new String(observation, UTF8));
      }
      return;
    }
    byte[] body = null;
    try {
      body = compress(sending);
      postBody(body);
    } catch (TransportException e) {
      if (REFUSED_STATUS.contains(e.status())) {
        refused(sending, body, e);
      } else {
        keep(body, sending.size(), e);
      }
      return;
    } catch (Exception e) {
      // Deliberately broad: see the class docstring. Anything escaping here surfaces inside the
      // customer's job.
      keep(body, sending.size(), e);
      return;
    }
    drain();
  }

  private void refused(List<byte[]> sending, byte[] body, TransportException e) {
    if (sending.size() > 1) {
      // One observation the receiver will not take must not cost the others in its batch.
      for (byte[] observation : sending) {
        send(Collections.singletonList(observation));
      }
      return;
    }
    File path = spool.save(body, Spool.REJECTED);
    LOG.warning("convalesce: an observation was refused (" + e.getMessage() + "); kept at " + path);
  }

  private void keep(byte[] body, int count, Exception e) {
    File path = body == null ? null : spool.save(body, Spool.PENDING);
    if (path != null) {
      LOG.warning(
          "convalesce: could not deliver "
              + count
              + " observation(s), kept at "
              + path
              + " to send again: "
              + e.getMessage());
    } else {
      LOG.severe(
          "convalesce: lost " + count + " observation(s), could not keep them: " + e.getMessage());
    }
  }

  private void drain() {
    List<File> waiting = spool.pending();
    for (int i = 0; i < waiting.size() && i < DRAIN_PER_SEND; i++) {
      File claimed = spool.claim(waiting.get(i));
      if (claimed == null) {
        continue;
      }
      try {
        postBody(Spool.read(claimed));
      } catch (TransportException e) {
        if (REFUSED_STATUS.contains(e.status())) {
          spool.reject(claimed);
          continue;
        }
        spool.release(claimed);
        return;
      } catch (Exception e) {
        spool.release(claimed);
        return;
      }
      Spool.done(claimed);
    }
  }

  /** Flushes anything still queued. */
  public void close() {
    flush();
  }

  private static byte[] compress(List<byte[]> sending) throws java.io.IOException {
    int size = BODY_OPEN.length() + BODY_CLOSE.length() + sending.size();
    for (byte[] observation : sending) {
      size += observation.length;
    }
    java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream(size);
    body.write(BODY_OPEN.getBytes(UTF8));
    for (int i = 0; i < sending.size(); i++) {
      if (i > 0) {
        body.write(',');
      }
      body.write(sending.get(i));
    }
    body.write(BODY_CLOSE.getBytes(UTF8));
    // Whole-payload forwarding means a batch is bigger than it used to be; gzip is what keeps the
    // wire cost from growing at the same rate. The receiver decides its size cap against the
    // decompressed bytes, so the batching above, sized off the uncompressed `bytes.length`, is
    // unaffected.
    return gzip(body.toByteArray());
  }

  private void postBody(byte[] compressed) throws Exception {
    String url = trimTrailingSlash(config.endpoint()) + OBSERVATIONS_PATH;

    for (int attempt = 0; attempt <= config.maxRetries(); attempt++) {
      try {
        attempt(url, compressed);
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
      // The key is the only thing here that says who is calling. No header
      // names an account.
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Encoding", "gzip");
      connection.setRequestProperty("Authorization", "Bearer " + config.ingestKey());
      connection.setRequestProperty("User-Agent", "convalesce-emit-java/" + Version.VERSION);
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

  private static byte[] gzip(byte[] data) throws java.io.IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(data.length);
    java.util.zip.GZIPOutputStream zipped = new java.util.zip.GZIPOutputStream(out);
    try {
      zipped.write(data);
    } finally {
      zipped.close();
    }
    return out.toByteArray();
  }

  static {
    // Spark's own logging is noisy enough; ours should be visible but not chatty.
    LOG.setLevel(Level.INFO);
  }
}
