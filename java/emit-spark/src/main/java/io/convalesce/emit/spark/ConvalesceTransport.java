package io.convalesce.emit.spark;

import io.convalesce.emit.Emitter;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Forwards what OpenLineage-Spark computed to Convalesce, as the observation {@code openlineage}.
 *
 * <p>Spark's own events cannot give a receiver a job's exact lineage: the logical plan never
 * appears in them. OpenLineage-Spark walks that plan inside the driver, where it still exists, and
 * this is the transport it hands the result to. The payload is {@code {"run_event": <the
 * RunEvent>}}, rendered by OpenLineage's own serialiser, and nothing here reads a field of it
 * except to decide when to flush and what to drop when it is too large.
 *
 * <p>One thing is changed. OpenLineage copies the Spark settings a job asked it to capture, and
 * what it reads of the platform, without redacting them, so they are redacted here by the rule the
 * listener applies to Spark's own events.
 *
 * <p>Nothing may escape into the customer's job, the same rule the listener keeps.
 */
public final class ConvalesceTransport extends Transport {

  private static final Logger LOG = Logger.getLogger(ConvalesceTransport.class.getName());
  private static final String TOOL = "spark";
  static final String EVENT = "openlineage";
  // The facet OpenLineage-Spark fills with the whole serialised plan. It is the largest thing an
  // event carries and the only part a receiver can do without, since the lineage it implies is
  // already in the inputs, outputs and column lineage.
  static final String LOGICAL_PLAN = "spark.logicalPlan";

  private final Emitter emitter;
  private final String sparkVersion;
  // Read from the running Spark at the first event: the transport is built with no configuration
  // of Spark's, and may be built before there is one to ask.
  private volatile Redaction redaction;

  /** Built by {@link ConvalesceTransportBuilder}, sharing the listener's emitter. */
  public ConvalesceTransport() {
    this(SharedEmitter.emitter(), SharedEmitter.sparkVersion());
  }

  /**
   * Direct construction, for tests and embedders.
   *
   * @param emitter what to send through
   * @param sparkVersion the version to record on each observation
   */
  ConvalesceTransport(Emitter emitter, String sparkVersion) {
    this.emitter = emitter;
    this.sparkVersion = sparkVersion;
  }

  @Override
  public void emit(OpenLineage.RunEvent event) {
    try {
      String payload = payload(event);
      if (!emitter.fits(TOOL, EVENT, payload, sparkVersion)) {
        payload = withoutLogicalPlan(event, payload);
      }
      emitter.emit(TOOL, EVENT, payload, sparkVersion);
      if (ends(event)) {
        // The driver may exit straight after a run ends, and a queued event would go with it.
        emitter.flush();
      }
    } catch (Throwable t) {
      LOG.warning("convalesce: could not emit an OpenLineage event: " + t.getMessage());
    }
  }

  /** OpenLineage-Spark sends none of these; a receiver reads run events only. */
  @Override
  public void emit(OpenLineage.DatasetEvent event) {
    LOG.fine("convalesce: not forwarding an OpenLineage dataset event");
  }

  /** OpenLineage-Spark sends none of these; a receiver reads run events only. */
  @Override
  public void emit(OpenLineage.JobEvent event) {
    LOG.fine("convalesce: not forwarding an OpenLineage job event");
  }

  @Override
  public void close() {
    try {
      emitter.flush();
    } catch (Throwable t) {
      LOG.warning("convalesce: could not flush on close: " + t.getMessage());
    }
  }

  private String payload(OpenLineage.RunEvent event) {
    Redaction rule = redaction;
    if (rule == null) {
      rule = Redaction.ofRunningSpark();
      redaction = rule;
    }
    return "{\"run_event\":" + rule.applyToRunEvent(OpenLineageClientUtils.toJson(event)) + "}";
  }

  /**
   * Renders the event without its logical plan, leaving the event as OpenLineage built it.
   *
   * <p>The facet is taken out only while serialising and put back after: another transport in a
   * composite may be handed the same object.
   */
  private String withoutLogicalPlan(OpenLineage.RunEvent event, String payload) {
    Map<String, OpenLineage.RunFacet> facets = runFacets(event);
    if (facets == null || !facets.containsKey(LOGICAL_PLAN)) {
      return payload;
    }
    OpenLineage.RunFacet plan;
    try {
      plan = facets.remove(LOGICAL_PLAN);
    } catch (UnsupportedOperationException e) {
      // Sent whole: the emitter sends it alone, and a refusal costs this one event only.
      return payload;
    }
    try {
      String smaller = payload(event);
      LOG.warning(
          "convalesce: an OpenLineage event was "
              + payload.length()
              + " characters, over the body limit; sent without "
              + LOGICAL_PLAN
              + " at "
              + smaller.length());
      return smaller;
    } finally {
      facets.put(LOGICAL_PLAN, plan);
    }
  }

  private static Map<String, OpenLineage.RunFacet> runFacets(OpenLineage.RunEvent event) {
    if (event.getRun() == null || event.getRun().getFacets() == null) {
      return null;
    }
    return event.getRun().getFacets().getAdditionalProperties();
  }

  private static boolean ends(OpenLineage.RunEvent event) {
    OpenLineage.RunEvent.EventType type = event.getEventType();
    return type == OpenLineage.RunEvent.EventType.COMPLETE
        || type == OpenLineage.RunEvent.EventType.FAIL
        || type == OpenLineage.RunEvent.EventType.ABORT;
  }
}
