package io.convalesce.emit.spark;

import io.convalesce.emit.Config;
import io.convalesce.emit.Emitter;
import io.convalesce.emit.Exclusion;
import io.convalesce.emit.Settings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorExcluded;
import org.apache.spark.scheduler.SparkListenerExecutorExcludedForStage;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerExecutorUnexcluded;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerNodeExcluded;
import org.apache.spark.scheduler.SparkListenerNodeExcludedForStage;
import org.apache.spark.scheduler.SparkListenerNodeUnexcluded;
import org.apache.spark.scheduler.SparkListenerResourceProfileAdded;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageExecutorMetrics;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.scheduler.SparkListenerUnschedulableTaskSetAdded;
import org.apache.spark.scheduler.SparkListenerUnschedulableTaskSetRemoved;

/**
 * Forwards Spark's own listener events to Convalesce, unchanged.
 *
 * <p>Wire it up with two lines of Spark config:
 *
 * <pre>
 * spark.jars.packages   io.convalesce:convalesce-emit-spark_2.12:0.1.8
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
 * <p><b>Which values.</b> A job start carries the job's whole Spark configuration, credentials
 * included, and Spark redacts them only on the way into its own event log. They are redacted here
 * by the same rule before anything leaves the driver.
 *
 * <p><b>Settings.</b> With {@code CONVALESCE_SEND_SETTINGS} on, the application's end also carries
 * the driver's environment variables under {@code settings}, a secret among them only as a keyed
 * hash; see {@link Settings}. It is the one field here that Spark did not write.
 *
 * <p><b>Lineage.</b> None of Spark's events carries the logical plan, so exact tables and columns
 * come from OpenLineage-Spark, which the {@code _2.12} and {@code _2.13} packages bring with them.
 * Where the job has not set OpenLineage up itself, this starts it and passes it every event; see
 * {@link CarriedOpenLineage}.
 *
 * <p>Nothing may escape into the customer's job. Every override wraps its body, and a failure to
 * emit is logged and dropped.
 */
public class ConvalesceSparkListener extends SparkListener {

  private static final Logger LOG = Logger.getLogger(ConvalesceSparkListener.class.getName());
  private static final String TOOL = "spark";
  private static final String INGEST_KEY = "CONVALESCE_INGEST_KEY";

  private final Emitter emitter;
  private final String sparkVersion;
  private final SparkEvents events = SparkEvents.fromEnvironment();
  private final Redaction redaction;
  // OpenLineage's own listener, when this started it; null when the job runs its own or none.
  private final SparkListenerInterface lineage;
  // Cleared when OpenLineage turns out to be built for another Scala or Spark than this driver's.
  private volatile boolean carrying = true;
  // Written by the listener bus thread and read by it; volatile so a later event on another
  // thread, which Spark does not promise against, still sees it.
  private volatile String appId;
  // The driver's environment: where this library's settings are read, and what is sent as the
  // run's settings when they are asked for.
  private final Map<String, String> env;

  /** Built by Spark when no constructor takes a SparkConf. */
  public ConvalesceSparkListener() {
    this(SharedEmitter.emitter(), SharedEmitter.sparkVersion());
  }

  /**
   * Built by Spark when {@code spark.extraListeners} names a class taking a conf, which Spark
   * prefers.
   *
   * @param conf the running job's configuration, read for its {@code spark.redaction.regex} and for
   *     whether the job set OpenLineage up itself
   */
  public ConvalesceSparkListener(SparkConf conf) {
    this(
        SharedEmitter.emitter(),
        SharedEmitter.sparkVersion(),
        Redaction.of(conf == null ? null : conf.get(Redaction.REGEX_KEY, null)),
        CarriedOpenLineage.start(conf));
  }

  /**
   * Direct construction, for tests and embedders.
   *
   * @param emitter what to send through
   * @param sparkVersion the version to record on each observation
   */
  public ConvalesceSparkListener(Emitter emitter, String sparkVersion) {
    this(emitter, sparkVersion, Redaction.of(null), null);
  }

  ConvalesceSparkListener(
      Emitter emitter, String sparkVersion, Redaction redaction, SparkListenerInterface lineage) {
    this(emitter, sparkVersion, redaction, lineage, System.getenv());
  }

  ConvalesceSparkListener(
      Emitter emitter,
      String sparkVersion,
      Redaction redaction,
      SparkListenerInterface lineage,
      Map<String, String> env) {
    this.emitter = emitter;
    this.sparkVersion = sparkVersion;
    this.redaction = redaction;
    this.lineage = lineage;
    this.env = env;
    if (!SparkEventJson.available()) {
      LOG.warning(
          "convalesce: this Spark has no serialiser we recognise; events will carry only their type");
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onApplicationStart(event));
    }
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onApplicationEnd(event));
    }
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
    if (lineage != null) {
      carry(() -> lineage.onJobStart(event));
    }
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onJobEnd(event));
    }
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted event) {
    // Stages are off by default, but a failed one carries the reason the job died; that is never
    // left behind.
    forward(event, failed(event));
    if (lineage != null) {
      carry(() -> lineage.onStageCompleted(event));
    }
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd event) {
    forward(event, failed(event));
    if (lineage != null) {
      carry(() -> lineage.onTaskEnd(event));
    }
  }

  static boolean failed(SparkListenerStageCompleted event) {
    try {
      return event.stageInfo().failureReason().isDefined();
    } catch (Throwable t) {
      return false;
    }
  }

  static boolean failed(SparkListenerTaskEnd event) {
    try {
      return !(event.reason() instanceof org.apache.spark.Success$);
    } catch (Throwable t) {
      return false;
    }
  }

  // Every other callback Spark has, so that `CONVALESCE_SPARK_EVENTS` can ask for any event Spark
  // posts: an event with a callback of its own never reaches `onOtherEvent`. Each is dropped by the
  // default filter before it is serialised. The Blacklisted twins of the Excluded events are left
  // out on purpose; see spark-events.yml beside the tests.

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onStageSubmitted(event));
    }
  }

  @Override
  public void onTaskStart(SparkListenerTaskStart event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onTaskStart(event));
    }
  }

  @Override
  public void onTaskGettingResult(SparkListenerTaskGettingResult event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onTaskGettingResult(event));
    }
  }

  @Override
  public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onEnvironmentUpdate(event));
    }
  }

  @Override
  public void onBlockManagerAdded(SparkListenerBlockManagerAdded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onBlockManagerAdded(event));
    }
  }

  @Override
  public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onBlockManagerRemoved(event));
    }
  }

  @Override
  public void onUnpersistRDD(SparkListenerUnpersistRDD event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onUnpersistRDD(event));
    }
  }

  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorMetricsUpdate(event));
    }
  }

  @Override
  public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onStageExecutorMetrics(event));
    }
  }

  @Override
  public void onExecutorAdded(SparkListenerExecutorAdded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorAdded(event));
    }
  }

  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorRemoved(event));
    }
  }

  @Override
  public void onExecutorExcluded(SparkListenerExecutorExcluded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorExcluded(event));
    }
  }

  @Override
  public void onExecutorExcludedForStage(SparkListenerExecutorExcludedForStage event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorExcludedForStage(event));
    }
  }

  @Override
  public void onNodeExcludedForStage(SparkListenerNodeExcludedForStage event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onNodeExcludedForStage(event));
    }
  }

  @Override
  public void onExecutorUnexcluded(SparkListenerExecutorUnexcluded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onExecutorUnexcluded(event));
    }
  }

  @Override
  public void onNodeExcluded(SparkListenerNodeExcluded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onNodeExcluded(event));
    }
  }

  @Override
  public void onNodeUnexcluded(SparkListenerNodeUnexcluded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onNodeUnexcluded(event));
    }
  }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onBlockUpdated(event));
    }
  }

  @Override
  public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onSpeculativeTaskSubmitted(event));
    }
  }

  @Override
  public void onUnschedulableTaskSetAdded(SparkListenerUnschedulableTaskSetAdded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onUnschedulableTaskSetAdded(event));
    }
  }

  @Override
  public void onUnschedulableTaskSetRemoved(SparkListenerUnschedulableTaskSetRemoved event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onUnschedulableTaskSetRemoved(event));
    }
  }

  @Override
  public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
    forward(event);
    if (lineage != null) {
      carry(() -> lineage.onResourceProfileAdded(event));
    }
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
    if (lineage != null) {
      carry(() -> lineage.onOtherEvent(event));
    }
  }

  private void forward(SparkListenerEvent event) {
    forward(event, false);
  }

  private void forward(SparkListenerEvent event, boolean always) {
    try {
      String name = event.getClass().getSimpleName();
      if (!always && !events.wanted(name)) {
        return;
      }
      String json = SparkEventJson.toJson(event);
      List<Exclusion> masked = new ArrayList<Exclusion>();
      json = SparkEventJson.withAppId(redaction.apply(json, masked), remember(json));
      if (event instanceof SparkListenerApplicationEnd) {
        // After the redaction, which is for what Spark wrote: this field has already had its
        // secrets hashed, by a rule a job's `spark.redaction.regex` knows nothing of.
        json = withSettings(json, env, masked);
      }
      emitter.emit(TOOL, name, json, sparkVersion, masked);
    } catch (Throwable t) {
      // A job must not fail because we could not report on it.
      LOG.warning("convalesce: could not emit an event: " + t.getMessage());
    }
  }

  /**
   * Adds the driver's environment to the event that ends a run, when settings are asked for.
   *
   * <p>The application's end, because it is the run's last event and the only one sent once for the
   * whole driver, and because the environment is the driver's and not any one job's.
   *
   * @param json the application-end event, redacted
   * @param env the driver's environment
   * @param excluded where what was left out of the settings is added, for the observation
   * @return the event with a {@code settings} field, or as it was when there is nothing to send
   */
  static String withSettings(String json, Map<String, String> env, List<Exclusion> excluded) {
    String ingestKey = Config.setting(env, INGEST_KEY);
    Settings.Collected found =
        Settings.collect(
            ingestKey == null ? null : ingestKey.trim(),
            Collections.singletonMap(Settings.ENVIRONMENT, env),
            env);
    excluded.addAll(found.excluded());
    return SparkEventJson.withField(json, Settings.FIELD, found.json());
  }

  /**
   * Passes one event on to OpenLineage, whatever {@code CONVALESCE_SPARK_EVENTS} says of it.
   *
   * @param call the callback Spark would have made, had it been registered
   */
  private void carry(Runnable call) {
    if (!carrying) {
      return;
    }
    try {
      call.run();
    } catch (LinkageError e) {
      // A method or class that is not there will not be there for the next event either, and a
      // warning per event would bury the job's own log.
      carrying = false;
      LOG.warning(
          "convalesce: the OpenLineage-Spark on the classpath was built for another Scala or "
              + "Spark than this driver runs, so table and column lineage is off for this job. "
              + "Use the convalesce-emit-spark package whose suffix is this Spark's Scala build. "
              + e);
    } catch (Throwable t) {
      LOG.warning("convalesce: OpenLineage could not read an event: " + t);
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
}
