package io.convalesce.emit;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.UUID;

/**
 * One thing a job tells us, wrapped for transport.
 *
 * <p>The payload is the tool's own output and crosses untouched. Every other field exists so the
 * receiver knows what it is holding: which tool produced it, which callback fired, and when.
 * Nothing here parses or reshapes the payload, which is what lets us improve how it is understood
 * without a customer upgrading anything.
 *
 * <p>The field names and their meanings match the Python client's envelope exactly, so one receiver
 * reads both.
 */
public final class Observation {

  /** Bumped only when the envelope's own shape changes, never for the payload inside it. */
  public static final int ENVELOPE_VERSION = 1;

  private final String tool;
  private final String event;
  private final String payloadJson;
  private final String toolVersion;
  private final String workspace;
  private final String observationId;
  private final String emittedAt;

  /**
   * Wraps a tool's payload for transport.
   *
   * @param tool which tool produced this, such as {@code spark}
   * @param event which callback fired, such as {@code SparkListenerJobEnd}
   * @param payloadJson the tool's own output, already JSON, embedded verbatim
   * @param toolVersion the tool's version, where it could be read
   * @param workspace which account this belongs to
   */
  public Observation(
      String tool, String event, String payloadJson, String toolVersion, String workspace) {
    this.tool = tool;
    this.event = event;
    this.payloadJson = payloadJson;
    this.toolVersion = toolVersion;
    this.workspace = workspace;
    this.observationId = UUID.randomUUID().toString();
    this.emittedAt = nowUtc();
  }

  /**
   * Renders this observation as JSON.
   *
   * @return the envelope, with the payload embedded as-is
   */
  public String toJson() {
    StringBuilder out = new StringBuilder(payloadJson == null ? 256 : payloadJson.length() + 256);
    out.append("{\"envelope_version\":").append(ENVELOPE_VERSION);
    out.append(",\"observation_id\":").append(Json.quote(observationId));
    out.append(",\"emitted_at\":").append(Json.quote(emittedAt));
    out.append(",\"tool\":").append(Json.quote(tool));
    out.append(",\"tool_version\":").append(Json.quote(toolVersion));
    out.append(",\"event\":").append(Json.quote(event));
    out.append(",\"client_version\":").append(Json.quote(Version.VERSION));
    out.append(",\"workspace\":").append(Json.quote(workspace));
    // Verbatim: this is the tool's own JSON, and re-encoding it would be the one thing this
    // package exists not to do.
    out.append(",\"payload\":").append(payloadJson == null ? "null" : payloadJson);
    out.append('}');
    return out.toString();
  }

  public String event() {
    return event;
  }

  private static String nowUtc() {
    // SimpleDateFormat rather than java.time: this targets Java 8 bytecode so it can load in any
    // Spark cluster, and a formatter built per observation costs nothing at this rate.
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    format.setTimeZone(new SimpleTimeZone(0, "UTC"));
    return format.format(new Date());
  }
}
