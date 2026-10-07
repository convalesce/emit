package io.convalesce.emit.spark;

import io.convalesce.emit.Config;
import java.io.File;
import java.lang.ref.WeakReference;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
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
  // OpenLineage copies only spark.master and spark.app.name unless told otherwise. The two Glue
  // settings name the job and the run even when the script gives its application another name;
  // off Glue they are absent and nothing is copied for them.
  static final String CAPTURED_PROPERTIES = "spark.openlineage.capturedProperties";
  static final String CAPTURED =
      "spark.master,spark.app.name,spark.glue.JOB_NAME,spark.glue.JOB_RUN_ID";
  // The variables AWS's own OpenLineage setup for Glue asks for, in OpenLineage's list syntax.
  static final String ENVIRONMENT_VARIABLES =
      "spark.openlineage.facets.custom_environment_variables";
  static final String GLUE_VARIABLES =
      "[AWS_DEFAULT_REGION;GLUE_VERSION;GLUE_COMMAND_CRITERIA;GLUE_PYTHON_VERSION;]";
  static final String GLUE_VERSION_KEY = "spark.glue.GLUE_VERSION";

  private static final String EXTRA_LISTENERS = "spark.extraListeners";
  private static final String TRANSPORT_PREFIX = "spark.openlineage.transport.";

  // The configuration OpenLineage was last started for, so two of our listeners on one context
  // start it once. Weak, because a driver can stop one context and build another.
  private static volatile WeakReference<SparkConf> started = new WeakReference<SparkConf>(null);
  // The configuration a job was last told there is no lineage for, so it is told once.
  private static volatile WeakReference<SparkConf> told = new WeakReference<SparkConf>(null);

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
      if (started.get() == live) {
        LOG.fine("convalesce: not starting OpenLineage: already started");
        return null;
      }
      String off = switchedOff(env);
      if (off != null) {
        LOG.fine("convalesce: not starting OpenLineage: " + off);
        return null;
      }
      String configured = configuredByJob(live, env);
      if (configured != null) {
        tellOnce(
            live,
            Level.INFO,
            "convalesce: OpenLineage is the job's own (set by "
                + configured
                + "), so its table and column lineage goes where the job sends it and not to"
                + " Convalesce; add the convalesce transport to send it here too");
        return null;
      }
      Class<?> found = find(listenerClass);
      if (found == null) {
        tellOnce(
            live,
            Level.WARNING,
            "convalesce: no table or column lineage for this job: openlineage-spark is not on"
                + " the classpath. Use the convalesce-emit-spark_2.12 or _2.13 package, or add"
                + " the openlineage-spark jar beside this one");
        return null;
      }
      live.set(TRANSPORT_TYPE, ConvalesceTransportBuilder.TYPE);
      if (!live.contains(DATASET_LINEAGE)) {
        live.set(DATASET_LINEAGE, "true");
      }
      if (!live.contains(CAPTURED_PROPERTIES)) {
        live.set(CAPTURED_PROPERTIES, CAPTURED);
      }
      if (onGlue(live, env) && !live.contains(ENVIRONMENT_VARIABLES)) {
        live.set(ENVIRONMENT_VARIABLES, GLUE_VARIABLES);
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

  private static String switchedOff(Map<String, String> env) {
    if (isFalse(Config.setting(env, OPT_OUT))) {
      return OPT_OUT + "=false";
    }
    if (isFalse(Config.setting(env, "CONVALESCE_ENABLED"))) {
      return "CONVALESCE_ENABLED=false";
    }
    return null;
  }

  static boolean onGlue(SparkConf conf, Map<String, String> env) {
    return conf.contains(GLUE_VERSION_KEY) || !blank(env.get("GLUE_VERSION"));
  }

  // Visible at the default level: a job whose lineage is missing should not need debug logging to
  // learn why. Once, because a driver may build this listener more than once.
  private static void tellOnce(SparkConf conf, Level level, String message) {
    if (told.get() == conf) {
      return;
    }
    told = new WeakReference<SparkConf>(conf);
    LOG.log(level, message);
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
