package io.convalesce.emit;

/**
 * Where to send observations, and how hard to try.
 *
 * <p>Read from the environment by default so a job picks it up without editing code: an operator
 * sets the variables once on the cluster, and every job in it emits without knowing this exists.
 * The variable names match the Python client's exactly, so one set of docs covers both.
 *
 * <p>The ingest key is held as a plain string. It is never logged and never placed in the envelope;
 * it appears only in the Authorization header.
 */
public final class Config {

  /** Default endpoint, used when {@code CONVALESCE_ENDPOINT} is unset. */
  public static final String DEFAULT_ENDPOINT = "https://api.convalesce.io";

  private static final int DEFAULT_TIMEOUT_MS = 10_000;
  private static final int DEFAULT_MAX_RETRIES = 3;
  // Fifty keeps a busy driver to roughly one request a second while staying small enough that a
  // lost batch costs little.
  private static final int DEFAULT_BATCH_SIZE = 50;

  private final String endpoint;
  private final String ingestKey;
  private final String workspace;
  private final int timeoutMs;
  private final int maxRetries;
  private final int batchSize;
  private final boolean dryRun;
  private final boolean enabled;

  private Config(
      String endpoint,
      String ingestKey,
      String workspace,
      int timeoutMs,
      int maxRetries,
      int batchSize,
      boolean dryRun,
      boolean enabled) {
    this.endpoint = endpoint;
    this.ingestKey = ingestKey;
    this.workspace = workspace;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
    this.batchSize = batchSize;
    this.dryRun = dryRun;
    this.enabled = enabled;
  }

  /**
   * Reads configuration from the process environment.
   *
   * @return the configuration, which may be unusable; call {@link #validate()} to find out
   */
  public static Config fromEnvironment() {
    return new Config(
        orDefault(readString("CONVALESCE_ENDPOINT"), DEFAULT_ENDPOINT),
        readString("CONVALESCE_INGEST_KEY"),
        readString("CONVALESCE_WORKSPACE"),
        readInt("CONVALESCE_TIMEOUT", DEFAULT_TIMEOUT_MS / 1000) * 1000,
        readInt("CONVALESCE_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        readInt("CONVALESCE_BATCH_SIZE", DEFAULT_BATCH_SIZE),
        readBoolean("CONVALESCE_DRY_RUN", false),
        readBoolean("CONVALESCE_ENABLED", true));
  }

  /**
   * Builds a configuration directly, for tests and embedders.
   *
   * @param endpoint base URL to post observations to
   * @param ingestKey write-only key a job presents; never logged
   * @param workspace which account these observations belong to
   * @param batchSize observations to hold before sending
   * @param maxRetries attempts after the first, for transient failures
   * @return the configuration
   */
  public static Config of(
      String endpoint, String ingestKey, String workspace, int batchSize, int maxRetries) {
    return new Config(
        endpoint, ingestKey, workspace, DEFAULT_TIMEOUT_MS, maxRetries, batchSize, false, true);
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
    return null;
  }

  public String endpoint() {
    return endpoint;
  }

  public String ingestKey() {
    return ingestKey;
  }

  public String workspace() {
    return workspace;
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

  public boolean dryRun() {
    return dryRun;
  }

  public boolean enabled() {
    return enabled;
  }

  private static String readString(String name) {
    String value = System.getenv(name);
    return value == null ? null : value.trim();
  }

  private static String orDefault(String value, String fallback) {
    return value == null || value.isEmpty() ? fallback : value;
  }

  private static boolean readBoolean(String name, boolean fallback) {
    String raw = readString(name);
    if (raw == null || raw.isEmpty()) {
      return fallback;
    }
    String lower = raw.toLowerCase();
    return lower.equals("1") || lower.equals("true") || lower.equals("yes") || lower.equals("on");
  }

  private static int readInt(String name, int fallback) {
    String raw = readString(name);
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
