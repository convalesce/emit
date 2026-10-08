package io.convalesce.emit;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.UUID;

/**
 * One thing a job tells us, wrapped for transport.
 *
 * <p>The payload is the tool's own output and crosses untouched. Every other field exists to say
 * what it is: which tool produced it, which callback fired, and when. Who sent it is not among
 * them; the ingest key carries that. Nothing here parses or reshapes the payload, which is what
 * lets how it is understood improve without a customer upgrading anything.
 *
 * <p>The field names and their meanings match the Python client's envelope exactly.
 */
public final class Observation {

  /**
   * Bumped only when the envelope's own shape changes, never for the payload inside it.
   *
   * <p>Version 2 adds {@code excluded}, naming anything left out of the payload by path and reason.
   * This client's payload is already the tool's own JSON (Spark's {@code JsonProtocol}), not walked
   * by anything of ours, so it sends an empty list here -- there is nothing of ours to declare an
   * exclusion about -- except when the emitter masked its own ingest key in the payload, or the
   * caller masked something in it and says so, which are declared. Collect keeps reading version 1
   * unchanged.
   */
  public static final int ENVELOPE_VERSION = 2;

  // The same path and reason the Python client declares.
  private static final Exclusion KEY_MASKED = new Exclusion("$", Exclusion.KEY_MASKED);

  private final String tool;
  private final String event;
  private final String payloadJson;
  private final String toolVersion;
  private final String observationId;
  private final String emittedAt;
  private final boolean keyMasked;
  private final List<Exclusion> excluded;

  /**
   * Wraps a tool's payload for transport.
   *
   * @param tool which tool produced this, such as {@code spark}
   * @param event which callback fired, such as {@code SparkListenerJobEnd}
   * @param payloadJson the tool's own output, already JSON, embedded verbatim
   * @param toolVersion the tool's version, where it could be read
   */
  public Observation(String tool, String event, String payloadJson, String toolVersion) {
    this(tool, event, payloadJson, toolVersion, false, Collections.<Exclusion>emptyList());
  }

  /**
   * Wraps a payload the emitter has looked through for its own ingest key.
   *
   * @param keyMasked whether the key was found in the payload and masked there
   * @param excluded what the caller masked in the payload before handing it over
   */
  Observation(
      String tool,
      String event,
      String payloadJson,
      String toolVersion,
      boolean keyMasked,
      List<Exclusion> excluded) {
    this.keyMasked = keyMasked;
    this.excluded = excluded;
    this.tool = tool;
    this.event = event;
    this.payloadJson = payloadJson;
    this.toolVersion = toolVersion;
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
    out.append(",\"excluded\":[");
    String separator = "";
    for (Exclusion exclusion : excluded) {
      out.append(separator).append(exclusion.toJson());
      separator = ",";
    }
    if (keyMasked) {
      out.append(separator).append(KEY_MASKED.toJson());
    }
    out.append(']');
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
