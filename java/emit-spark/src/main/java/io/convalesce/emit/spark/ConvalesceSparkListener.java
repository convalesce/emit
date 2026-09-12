package io.convalesce.emit.spark;

import io.convalesce.emit.Emitter;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * Forwards Spark's own listener events to Convalesce, unchanged.
 *
 * <p>Wire it up with two lines of Spark config:
 *
 * <pre>
 * spark.jars.packages   io.convalesce:convalesce-emit-spark:0.1.0
 * spark.extraListeners  io.convalesce.emit.spark.ConvalesceSparkListener
 * </pre>
 *
 * <p>Every callback does the same thing: hand the event to Spark's own serialiser and forward the
 * string. Nothing here reads a field off an event, which is why one artifact covers Spark 3.0
 * through 4.x and both Scala builds: the only Spark types touched are the event classes themselves
 * and {@code JsonProtocol}, and none of them are Scala-version-specific in signature.
 *
 * <p>Two things are decided here rather than forwarded blindly.
 *
 * <p><b>Which events.</b> A run is described by the application, the jobs and the SQL executions
 * starting and ending. Task and stage events describe the inside of a job: one per task, about 190
 * values each, so a job with ten thousand tasks was ten thousand observations that no receiver
 * read. Those, and the adaptive plan updates, are sent only when {@code CONVALESCE_SPARK_EVENTS}
 * asks for them.
 *
 * <p><b>Which application.</b> Only the application-start event and a job's properties name the
 * application, so a job end, an application end or a SQL execution said nothing about which driver
 * it came from, and a receiver seeing two drivers at once had to guess. The id is remembered from
 * whichever event names it and stamped on every event that does not.
 *
 * <p>Nothing may escape into the customer's job. Every override wraps its body, and a failure to
 * emit is logged and dropped.
 */
public class ConvalesceSparkListener extends SparkListener {

  private static final Logger LOG = Logger.getLogger(ConvalesceSparkListener.class.getName());
  private static final String TOOL = "spark";

  private final Emitter emitter;
  private final String sparkVersion;
  private final SparkEvents events = SparkEvents.fromEnvironment();
  // Written by the listener bus thread and read by it; volatile so a later event on another
  // thread, which Spark does not promise against, still sees it.
  private volatile String appId;

  /** Built by Spark when no constructor takes a SparkConf. */
  public ConvalesceSparkListener() {
    this(new Emitter(), readSparkVersion());
  }

  /**
   * Built by Spark when {@code spark.extraListeners} names a class taking a conf.
   *
   * @param conf the running job's configuration, unused but required by Spark's contract
   */
  public ConvalesceSparkListener(SparkConf conf) {
    this(new Emitter(), readSparkVersion());
  }

  /**
   * Direct construction, for tests and embedders.
   *
   * @param emitter what to send through
   * @param sparkVersion the version to record on each observation
   */
  public ConvalesceSparkListener(Emitter emitter, String sparkVersion) {
    this.emitter = emitter;
    this.sparkVersion = sparkVersion;
    if (!SparkEventJson.available()) {
      LOG.warning("convalesce: this Spark has no serialiser we recognise; nothing will be sent");
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart event) {
    forward(event);
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd event) {
    forward(event);
    // The driver is going away; anything still queued goes now or never.
    try {
      emitter.close();
    } catch (Throwable t) {
      LOG.warning("convalesce: could not flush on shutdown: " + t.getMessage());
    }
  }

  @Override
  public void onJobStart(SparkListenerJobStart event) {
    forward(event);
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd event) {
    forward(event);
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted event) {
    forward(event);
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd event) {
    forward(event);
  }

  /** The application this listener is reporting on, once anything has named it. */
  String applicationId() {
    return appId;
  }

  /**
   * Everything Spark does not have a dedicated callback for.
   *
   * <p>This is where SQL execution events arrive, which is where a job's inputs and outputs are
   * described, which is the reason a lineage integration exists at all.
   */
  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    forward(event);
  }

  private void forward(SparkListenerEvent event) {
    try {
      String name = event.getClass().getSimpleName();
      if (!events.wanted(name)) {
        return;
      }
      String json = SparkEventJson.toJson(event);
      if (json == null) {
        return;
      }
      json = SparkEventJson.withAppId(json, remember(json));
      emitter.emit(TOOL, name, json, sparkVersion);
    } catch (Throwable t) {
      // A job must not fail because we could not report on it.
      LOG.warning("convalesce: could not emit an event: " + t.getMessage());
    }
  }

  /**
   * Keeps the application id from whichever event names it.
   *
   * @param json the event as Spark rendered it
   * @return the id to stamp on this event, or null when none is known yet
   */
  private String remember(String json) {
    String found = SparkEventJson.readAppId(json);
    if (found != null) {
      appId = found;
    }
    return appId;
  }

  private static String readSparkVersion() {
    try {
      return org.apache.spark.package$.MODULE$.SPARK_VERSION();
    } catch (Throwable t) {
      // A version is a nicety, never a blocker.
      return null;
    }
  }
}
