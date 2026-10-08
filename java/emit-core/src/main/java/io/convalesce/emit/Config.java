package io.convalesce.emit;

import java.util.Map;

/**
 * Where to send observations, and how hard to try.
 *
 * <p>Read from the environment by default so a job picks it up without editing code: an operator
 * sets the variables once on the cluster, and every job in it emits without knowing this exists.
 * The variable names match the Python client's exactly, so one set of docs covers both.
 *
 * <p>There is no account or tenant setting, deliberately. The ingest key is what identifies the
 * caller, and it is the only thing that does. A separate setting naming the account would be an
 * unauthenticated claim sitting next to the credential that actually proves it, and the two could
 * disagree.
 *
 * <p>The ingest key is held as a plain string. It is never logged and never placed in the envelope;
 * it appears only in the Authorization header.
 */
public final class Config {

  /** Default endpoint, used when {@code CONVALESCE_ENDPOINT} is unset. */
  public static final String DEFAULT_ENDPOINT = "https://api.convalesce.io";

  /** What a platform may put in front of a setting's name; see {@link #setting(Map, String)}. */
  public static final String PLATFORM_PREFIX = "CUSTOMER_";

  private static final int DEFAULT_TIMEOUT_MS = 10_000;
  private static final int DEFAULT_MAX_RETRIES = 3;
  // Fifty keeps a busy driver to roughly one request a second while staying small enough that a
  // lost batch costs little.
  private static final int DEFAULT_BATCH_SIZE = 50;
  // The receiver takes at most fifty observations and five megabytes in one request and refuses the
  // whole request past either, and a refusal is not retried, so a configured batch may not exceed
  // them.
  static final int RECEIVER_MAX_OBSERVATIONS = 50;
  static final int RECEIVER_MAX_BODY_BYTES = 5_000_000;
  // Well under the receiver's limit, so a batch is closed long before a request could be
  // refused for its size.
  private static final int DEFAULT_MAX_BODY_BYTES = 1_000_000;
  // Where undelivered batches wait to be sent again, and how much disk they may take before a new
  // one is refused rather than filling it. The same directory the Python client uses.
  private static final String DEFAULT_SPOOL_DIR =
      new java.io.File(System.getProperty("java.io.tmpdir"), "convalesce-emit-spool").getPath();
  private static final long DEFAULT_SPOOL_MAX_BYTES = 1_000_000_000L;
  // A driver that runs for hours still sends what it has every few seconds, instead of holding a
  // part batch until fifty events or the application's end.
  private static final int DEFAULT_FLUSH_INTERVAL_MS = 5_000;

  private final String endpoint;
  private final String ingestKey;
  private final int timeoutMs;
  private final int maxRetries;
  private final int batchSize;
  private final int maxBodyBytes;
  private final boolean dryRun;
  private final boolean enabled;
  private final String spoolDir;
  private final long spoolMaxBytes;
  private final int flushIntervalMs;

  private Config(
      String endpoint,
      String ingestKey,
      int timeoutMs,
      int maxRetries,
      int batchSize,
      int maxBodyBytes,
      boolean dryRun,
      boolean enabled,
      String spoolDir,
      long spoolMaxBytes,
      int flushIntervalMs) {
    this.endpoint = endpoint;
    this.ingestKey = ingestKey;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
    this.batchSize = batchSize;
    this.maxBodyBytes = maxBodyBytes;
    this.dryRun = dryRun;
    this.enabled = enabled;
    this.spoolDir = spoolDir;
    this.spoolMaxBytes = spoolMaxBytes;
    this.flushIntervalMs = flushIntervalMs;
  }

  /**
   * Reads configuration from the process environment.
   *
   * @return the configuration, which may be unusable; call {@link #validate()} to find out
   */
  public static Config fromEnvironment() {
    return fromEnvironment(System.getenv());
  }

  /**
   * Reads configuration from an environment given, for tests and embedders.
   *
   * @param env the variables to read
   * @return the configuration, which may be unusable; call {@link #validate()} to find out
   */
  public static Config fromEnvironment(Map<String, String> env) {
    return new Config(
        orDefault(readString(env, "CONVALESCE_ENDPOINT"), DEFAULT_ENDPOINT),
        readString(env, "CONVALESCE_INGEST_KEY"),
        readInt(env, "CONVALESCE_TIMEOUT", DEFAULT_TIMEOUT_MS / 1000) * 1000,
        readInt(env, "CONVALESCE_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        readInt(env, "CONVALESCE_BATCH_SIZE", DEFAULT_BATCH_SIZE),
        readInt(env, "CONVALESCE_MAX_BODY_BYTES", DEFAULT_MAX_BODY_BYTES),
        readBoolean(env, "CONVALESCE_DRY_RUN", false),
        readBoolean(env, "CONVALESCE_ENABLED", true),
        orDefault(readString(env, "CONVALESCE_SPOOL_DIR"), DEFAULT_SPOOL_DIR),
        readLong(env, "CONVALESCE_SPOOL_MAX_BYTES", DEFAULT_SPOOL_MAX_BYTES),
        readInt(env, "CONVALESCE_FLUSH_INTERVAL", DEFAULT_FLUSH_INTERVAL_MS / 1000) * 1000);
  }

  /**
   * One setting from the process environment, under either of its names.
   *
   * @param name the setting, such as {@code CONVALESCE_ENDPOINT}
   * @return its value, or null when it is set under neither name
   */
  public static String setting(String name) {
    return setting(System.getenv(), name);
  }

  /**
   * One setting, under either of its names.
   *
   * <p>A platform that passes a job only the variables it prefixes, as AWS Glue does with {@code
   * CUSTOMER_}, can still configure it: {@code CUSTOMER_CONVALESCE_ENDPOINT} is read where {@code
   * CONVALESCE_ENDPOINT} is not set. Every setting is read through here, so none is left out.
   *
   * @param env the variables to read
   * @param name the setting, such as {@code CONVALESCE_ENDPOINT}
   * @return its value, or null when it is set under neither name
   */
  public static String setting(Map<String, String> env, String name) {
    String value = env.get(name);
    return value != null ? value : env.get(PLATFORM_PREFIX + name);
  }

  /**
   * Builds a configuration directly, for tests and embedders.
   *
   * @param endpoint base URL to post observations to
   * @param ingestKey write-only key a job presents; never logged
   * @param batchSize observations to hold before sending
   * @param maxRetries attempts after the first, for transient failures
   * @return the configuration
   */
  public static Config of(String endpoint, String ingestKey, int batchSize, int maxRetries) {
    return of(endpoint, ingestKey, batchSize, maxRetries, DEFAULT_MAX_BODY_BYTES);
  }

  /**
   * Builds a configuration directly, naming the receiver's body limit too.
   *
   * @param endpoint base URL to post observations to
   * @param ingestKey write-only key a job presents; never logged
   * @param batchSize observations to hold before sending
   * @param maxRetries attempts after the first, for transient failures
   * @param maxBodyBytes encoded size a batch is sent before reaching
   * @return the configuration
   */
  public static Config of(
      String endpoint, String ingestKey, int batchSize, int maxRetries, int maxBodyBytes) {
    return new Config(
        endpoint,
        ingestKey,
        DEFAULT_TIMEOUT_MS,
        maxRetries,
        batchSize,
        maxBodyBytes,
        false,
        true,
        DEFAULT_SPOOL_DIR,
        DEFAULT_SPOOL_MAX_BYTES,
        // Off: a caller building a configuration in code flushes when it chooses.
        0);
  }

  /**
   * The same configuration, keeping undelivered batches somewhere else.
   *
   * @param directory where the spool lives
   * @return the configuration
   */
  public Config withSpoolDir(String directory) {
    return new Config(
        endpoint,
        ingestKey,
        timeoutMs,
        maxRetries,
        batchSize,
        maxBodyBytes,
        dryRun,
        enabled,
        directory,
        spoolMaxBytes,
        flushIntervalMs);
  }

  /**
   * Checks that this configuration can actually send.
   *
   * @return null when usable, otherwise why it is not
   */
  public String validate() {
    // Neither mode reaches the network, so neither needs a key.
    if (!enabled || dryRun) {
      return null;
    }
    if (ingestKey == null || ingestKey.isEmpty()) {
      return "No ingest key. Set CONVALESCE_INGEST_KEY, or set CONVALESCE_DRY_RUN=true to build"
          + " envelopes without sending.";
    }
    if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
      return "CONVALESCE_ENDPOINT must be an http(s) URL, got: " + endpoint;
    }
    if (batchSize <= 0 || batchSize > RECEIVER_MAX_OBSERVATIONS) {
      return "CONVALESCE_BATCH_SIZE must be 1 to "
          + RECEIVER_MAX_OBSERVATIONS
          + ", the receiver's limit, got: "
          + batchSize;
    }
    if (maxBodyBytes <= 0 || maxBodyBytes > RECEIVER_MAX_BODY_BYTES) {
      return "CONVALESCE_MAX_BODY_BYTES must be 1 to "
          + RECEIVER_MAX_BODY_BYTES
          + ", the receiver's limit, got: "
          + maxBodyBytes;
    }
    return null;
  }

  public String endpoint() {
    return endpoint;
  }

  public String ingestKey() {
    return ingestKey;
  }

  public int timeoutMs() {
    return timeoutMs;
  }

  public int maxRetries() {
    return maxRetries;
  }

  public int batchSize() {
    return batchSize;
  }

  public int maxBodyBytes() {
    return maxBodyBytes;
  }

  public boolean dryRun() {
    return dryRun;
  }

  public boolean enabled() {
    return enabled;
  }

  public String spoolDir() {
    return spoolDir;
  }

  public long spoolMaxBytes() {
    return spoolMaxBytes;
  }

  /** Milliseconds between background flushes; zero or less turns them off. */
  public int flushIntervalMs() {
    return flushIntervalMs;
  }

  private static String readString(Map<String, String> env, String name) {
    String value = setting(env, name);
    return value == null ? null : value.trim();
  }

  private static String orDefault(String value, String fallback) {
    return value == null || value.isEmpty() ? fallback : value;
  }

  private static boolean readBoolean(Map<String, String> env, String name, boolean fallback) {
    String raw = readString(env, name);
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    String lower = raw.toLowerCase();
    return lower.equals("1") || lower.equals("true") || lower.equals("yes") || lower.equals("on");
  }

  private static long readLong(Map<String, String> env, String name, long fallback) {
    String raw = readString(env, name);
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    try {
      return (long) Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  private static int readInt(Map<String, String> env, String name, int fallback) {
    String raw = readString(env, name);
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    try {
      return (int) Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      // A misconfigured number must not stop the job; the default is safe.
      return fallback;
    }
  }
}
