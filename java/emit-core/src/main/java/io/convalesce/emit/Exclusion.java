package io.convalesce.emit;

/**
 * One thing left out of a payload, or masked in it, before it was sent: where, and why.
 *
 * <p>The same {@code {"path", "reason"}} the Python client declares in an envelope's {@code
 * excluded}.
 */
public final class Exclusion {

  /** The reason for Convalesce's own ingest key masked, by its value or the name it is under. */
  public static final String KEY_MASKED = "ingest key masked";

  private final String path;
  private final String reason;

  /**
   * Declares one exclusion.
   *
   * @param path where in the payload, such as {@code
   *     Properties.spark.glue.customer-driver-env-vars}
   * @param reason why, in a few words
   */
  public Exclusion(String path, String reason) {
    this.path = path;
    this.reason = reason;
  }

  public String path() {
    return path;
  }

  public String reason() {
    return reason;
  }

  String toJson() {
    return "{\"path\":" + Json.quote(path) + ",\"reason\":" + Json.quote(reason) + "}";
  }
}
