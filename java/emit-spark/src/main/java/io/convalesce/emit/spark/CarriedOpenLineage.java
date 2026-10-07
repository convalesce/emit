package io.convalesce.emit.spark;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.SparkListenerInterface;
import scala.Tuple2;

/**
 * Starts OpenLineage-Spark for a job that did not set it up, pointed at this jar's transport.
 *
 * <p>Spark's own events never carry the logical plan, so exact table and column lineage comes only
 * from OpenLineage-Spark walking that plan inside the driver. {@code convalesce-emit-spark_2.12}
 * and {@code _2.13} bring it as a dependency, so it is on the classpath of a job that named one
 * package; this is what makes it run without the job naming its listener or its settings too.
 *
 * <p>The OpenLineage listener is built here and handed to {@link ConvalesceSparkListener}, which
 * passes it every event Spark delivers. It is not registered with Spark: {@code
 * spark.extraListeners} has been read by the time a listener is constructed, and a listener added
 * to a running context can miss the application start. OpenLineage reads its settings from the
 * driver's own {@link SparkConf} when the application starts, which is after this has written them.
 *
 * <p>Never over a job's own setup: any OpenLineage transport, configuration file or switch the job
 * or its platform set leaves everything as it was. Nothing happens where OpenLineage-Spark is not
 * on the classpath, or where {@code CONVALESCE_OPENLINEAGE=false}.
 */
final class CarriedOpenLineage {

  private static final Logger LOG = Logger.getLogger(CarriedOpenLineage.class.getName());

  static final String LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener";
  static final String OPT_OUT = "CONVALESCE_OPENLINEAGE";
  static final String TRANSPORT_TYPE = "spark.openlineage.transport.type";
  // Off by default in OpenLineage, which then reports the columns a filter, join or grouping read
  // as inputs of every output column. On, they are reported once for the dataset.
  static final String DATASET_LINEAGE = "spark.openlineage.columnLineage.datasetLineageEnabled";

  private static final String EXTRA_LISTENERS = "spark.extraListeners";
  private static final String TRANSPORT_PREFIX = "spark.openlineage.transport.";

  // The configuration OpenLineage was last started for, so two of our listeners on one context
  // start it once. Weak, because a driver can stop one context and build another.
  private static volatile WeakReference<SparkConf> started = new WeakReference<SparkConf>(null);

  private CarriedOpenLineage() {}

  /**
   * Starts OpenLineage for the running driver, unless the job already chose.
   *
   * @param conf the configuration Spark built the listener with
   * @return OpenLineage's listener, to be passed every event; null when it was not started
   */
  static SparkListenerInterface start(SparkConf conf) {
    return start(conf, System.getenv(), LISTENER);
  }

  /**
   * The same, with the environment and the listener's class given, for tests.
   *
   * @param conf the configuration Spark built the listener with
   * @param env the process environment
   * @param listenerClass the class to start, which takes a {@link SparkConf}
   * @return the listener, or null when it was not started
   */
  static synchronized SparkListenerInterface start(
      SparkConf conf, Map<String, String> env, String listenerClass) {
    try {
      if (conf == null) {
        return null;
      }
      // What OpenLineage reads is the driver's own configuration. Spark hands a listener named in
      // spark.extraListeners that very object, but an embedder may hand over a copy.
      SparkConf live = liveConf(conf);
      String reason = skipReason(live, env);
      if (reason == null && started.get() == live) {
        reason = "already started";
      }
      Class<?> found = reason == null ? find(listenerClass) : null;
      if (reason == null && found == null) {
        reason = "openlineage-spark is not on the classpath";
      }
      if (reason != null) {
        LOG.fine("convalesce: not starting OpenLineage: " + reason);
        return null;
      }
      live.set(TRANSPORT_TYPE, ConvalesceTransportBuilder.TYPE);
      if (!live.contains(DATASET_LINEAGE)) {
        live.set(DATASET_LINEAGE, "true");
      }
      Object listener = found.getConstructor(SparkConf.class).newInstance(live);
      started = new WeakReference<SparkConf>(live);
      LOG.info("convalesce: started OpenLineage for table and column lineage");
      return (SparkListenerInterface) listener;
    } catch (Throwable t) {
      // A job must run whether or not its lineage can be read.
      LOG.warning("convalesce: could not start OpenLineage: " + t);
      return null;
    }
  }

  /**
   * Why OpenLineage must not be started here, if it must not.
   *
   * @param conf the driver's configuration
   * @param env the process environment
   * @return the reason, or null when it should be started
   */
  static String skipReason(SparkConf conf, Map<String, String> env) {
    if (isFalse(env.get(OPT_OUT))) {
      return OPT_OUT + "=false";
    }
    if (isFalse(env.get("CONVALESCE_ENABLED"))) {
      return "CONVALESCE_ENABLED=false";
    }
    String configured = configuredByJob(conf, env);
    return configured == null ? null : "already configured by " + configured;
  }

  /**
   * The first sign the job set OpenLineage up, or switched it off.
   *
   * @param conf the driver's configuration
   * @param env the process environment
   * @return what was found, or null when OpenLineage is untouched
   */
  static String configuredByJob(SparkConf conf, Map<String, String> env) {
    for (String each : conf.get(EXTRA_LISTENERS, "").split(",")) {
      if (each.trim().equals(LISTENER)) {
        return EXTRA_LISTENERS;
      }
    }
    for (Tuple2<String, String> setting : conf.getAll()) {
      String key = setting._1();
      boolean ours =
          key.equals(TRANSPORT_TYPE) && ConvalesceTransportBuilder.TYPE.equals(setting._2().trim());
      if (key.startsWith(TRANSPORT_PREFIX) && !ours) {
        return key;
      }
    }
    // The two settings OpenLineage took a backend from before it had transports.
    for (String key : new String[] {"spark.openlineage.url", "spark.openlineage.host"}) {
      if (conf.contains(key)) {
        return key;
      }
    }
    if (isTrue(conf.get("spark.openlineage.disabled", null))) {
      return "spark.openlineage.disabled";
    }
    for (String name : new String[] {"OPENLINEAGE_URL", "OPENLINEAGE_CONFIG"}) {
      if (!blank(env.get(name))) {
        return name;
      }
    }
    if (isTrue(env.get("OPENLINEAGE_DISABLED"))) {
      return "OPENLINEAGE_DISABLED";
    }
    // The OpenLineage client's own per-key variables.
    for (String name : env.keySet()) {
      if (name.startsWith("OPENLINEAGE__TRANSPORT")) {
        return "OPENLINEAGE__TRANSPORT*";
      }
    }
    // Where the client looks for its own file when nothing names one.
    for (String path :
        new String[] {
          new File(System.getProperty("user.dir", "."), "openlineage.yml").getPath(),
          new File(System.getProperty("user.home", "."), ".openlineage/openlineage.yml").getPath()
        }) {
      if (new File(path).isFile()) {
        return path;
      }
    }
    return null;
  }

  private static SparkConf liveConf(SparkConf given) {
    try {
      SparkEnv env = SparkEnv.get();
      if (env != null && env.conf() != null) {
        return env.conf();
      }
    } catch (Throwable t) {
      // Then the one Spark handed over is the only one there is.
    }
    return given;
  }

  private static Class<?> find(String name) {
    ClassLoader context = Thread.currentThread().getContextClassLoader();
    for (ClassLoader loader :
        new ClassLoader[] {context, CarriedOpenLineage.class.getClassLoader()}) {
      try {
        return Class.forName(name, true, loader);
      } catch (ClassNotFoundException | LinkageError e) {
        // Try the next; absent from both means it was never added.
      }
    }
    return null;
  }

  private static boolean blank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static boolean isFalse(String value) {
    String lower = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    return lower.equals("0") || lower.equals("false") || lower.equals("no") || lower.equals("off");
  }

  private static boolean isTrue(String value) {
    String lower = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    return lower.equals("1") || lower.equals("true") || lower.equals("yes") || lower.equals("on");
  }
}
