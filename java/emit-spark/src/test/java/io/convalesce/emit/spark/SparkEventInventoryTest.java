package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.junit.Test;

/**
 * Every event the Spark on this classpath can post, and what the listener does with it.
 *
 * <p>Each concrete {@link SparkListenerEvent} in Spark's own jars (core, catalyst, SQL and whatever
 * other {@code spark-*} jar is on the test classpath) must be forwarded by default, reachable
 * through {@code CONVALESCE_SPARK_EVENTS}, or listed in {@code spark-events.yml} with a reason.
 * Bumping the Spark the tests run against is how a new event is found: this fails until somebody
 * decides what it is.
 *
 * <p>"Reachable" is decided the way Spark decides it: {@code SparkListenerBus} hands an event to
 * the callback whose parameter type it is, and only an event with no callback of its own to {@code
 * onOtherEvent}. An event whose callback the listener does not override never arrives, whatever the
 * setting says.
 */
public class SparkEventInventoryTest {

  private static final String EXCLUSIONS = "/spark-events.yml";

  @Test
  public void everyEventSparkPostsIsForwardedOrExcludedWithAReason() throws IOException {
    Map<String, String> excluded = exclusions();
    SparkEvents byDefault = SparkEvents.of(null);
    SparkEvents everything = SparkEvents.of("all");
    List<String> unclassified = new ArrayList<String>();
    List<String> staleExclusions = new ArrayList<String>();
    TreeSet<String> seen = new TreeSet<String>();
    for (Class<?> event : sparkEvents()) {
      String name = event.getSimpleName();
      seen.add(name);
      boolean arrives = arrives(event);
      boolean forwarded = arrives && (byDefault.wanted(name) || everything.wanted(name));
      if (excluded.containsKey(name)) {
        if (arrives) {
          staleExclusions.add(name + " (" + callbackFor(event).getName() + " is overridden)");
        }
      } else if (!forwarded) {
        unclassified.add(event.getName() + " arrives on " + callbackFor(event).getName());
      }
    }
    for (String name : excluded.keySet()) {
      if (!seen.contains(name)) {
        staleExclusions.add(name + " (not an event of this Spark)");
      }
    }
    if (!unclassified.isEmpty()) {
      fail(
          "Spark posts events the listener neither receives nor excludes; override their "
              + "callback in ConvalesceSparkListener or list them in spark-events.yml with a "
              + "reason:\n  "
              + String.join("\n  ", unclassified));
    }
    assertTrue(
        "spark-events.yml excludes events it should not:\n  "
            + String.join("\n  ", staleExclusions),
        staleExclusions.isEmpty());
  }

  @Test
  public void theDefaultSetNamesEventsThisSparkStillPosts() throws IOException {
    TreeSet<String> names = new TreeSet<String>();
    for (Class<?> event : sparkEvents()) {
      if (arrives(event)) {
        names.add(event.getSimpleName());
      }
    }
    for (String name :
        new String[] {
          "SparkListenerApplicationStart",
          "SparkListenerApplicationEnd",
          "SparkListenerJobStart",
          "SparkListenerJobEnd",
          "SparkListenerSQLExecutionStart",
          "SparkListenerSQLExecutionEnd"
        }) {
      assertTrue(name + " is forwarded by default", SparkEvents.of(null).wanted(name));
      assertTrue(name + " is no longer an event this Spark delivers", names.contains(name));
    }
  }

  @Test
  public void theInventoryReallyFoundSparksEvents() throws IOException {
    // A classpath scan that silently finds nothing would pass the test above.
    TreeSet<String> names = new TreeSet<String>();
    for (Class<?> event : sparkEvents()) {
      names.add(event.getSimpleName());
    }
    assertTrue(names.toString(), names.contains("SparkListenerTaskEnd"));
    assertTrue(names.toString(), names.contains("SparkListenerSQLAdaptiveExecutionUpdate"));
    assertTrue(names.toString(), names.contains("QueryStartedEvent"));
    assertTrue(names.toString(), names.contains("CreateTableEvent"));
  }

  /** Whether Spark would hand this event to a method the listener overrides. */
  private static boolean arrives(Class<?> event) {
    Method callback = callbackFor(event);
    try {
      ConvalesceSparkListener.class.getDeclaredMethod(
          callback.getName(), callback.getParameterTypes());
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /** The {@code SparkListenerInterface} method Spark's bus calls for this event. */
  private static Method callbackFor(Class<?> event) {
    Method other = null;
    for (Method method : SparkListenerInterface.class.getMethods()) {
      Class<?>[] parameters = method.getParameterTypes();
      if (parameters.length != 1) {
        continue;
      }
      if (parameters[0] == SparkListenerEvent.class) {
        other = method;
      } else if (parameters[0].isAssignableFrom(event)) {
        return method;
      }
    }
    assertTrue("SparkListenerInterface has no onOtherEvent", other != null);
    return other;
  }

  /** Every concrete event class in the {@code spark-*} jars on this classpath. */
  private static List<Class<?>> sparkEvents() throws IOException {
    List<Class<?>> out = new ArrayList<Class<?>>();
    ClassLoader loader = SparkEventInventoryTest.class.getClassLoader();
    int jars = 0;
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      File file = new File(entry);
      if (!file.getName().startsWith("spark-") || !file.getName().endsWith(".jar")) {
        continue;
      }
      jars++;
      try (JarFile jar = new JarFile(file)) {
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
          String path = entries.nextElement().getName();
          if (!path.startsWith("org/apache/spark/") || !path.endsWith(".class")) {
            continue;
          }
          Class<?> type = load(loader, path);
          if (type != null
              && SparkListenerEvent.class.isAssignableFrom(type)
              && !type.isInterface()
              && !Modifier.isAbstract(type.getModifiers())
              && !type.isAnonymousClass()
              && !type.getSimpleName().isEmpty()
              // A Scala `object` extending an event trait is its companion, not an event.
              && !type.getName().endsWith("$")) {
            out.add(type);
          }
        }
      }
    }
    assertTrue("no spark-* jars on the test classpath", jars > 0);
    return out;
  }

  private static Class<?> load(ClassLoader loader, String path) {
    String name = path.substring(0, path.length() - ".class".length()).replace('/', '.');
    try {
      return Class.forName(name, false, loader);
    } catch (Throwable t) {
      // A class whose optional dependencies are not on the test classpath (Hive, Kubernetes)
      // cannot be an event this listener sees without them either.
      return null;
    }
  }

  /**
   * The events excluded on purpose, each with its reason.
   *
   * <p>A flat YAML mapping under {@code excluded:}, read by hand to keep a YAML library off the
   * test classpath of a jar that ships none.
   */
  static Map<String, String> exclusions() throws IOException {
    Map<String, String> out = new TreeMap<String, String>();
    InputStream in = SparkEventInventoryTest.class.getResourceAsStream(EXCLUSIONS);
    assertTrue(EXCLUSIONS + " is missing", in != null);
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      boolean inside = false;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty() || line.trim().startsWith("#")) {
          continue;
        }
        if (!line.startsWith(" ")) {
          inside = line.trim().equals("excluded:");
          continue;
        }
        if (!inside) {
          continue;
        }
        int colon = line.indexOf(':');
        assertTrue("not `Name: reason`: " + line, colon > 0);
        String name = line.substring(0, colon).trim();
        String reason = unquote(line.substring(colon + 1).trim());
        assertTrue(name + " is excluded without a reason", !reason.isEmpty());
        assertEquals(name + " is listed twice", null, out.put(name, reason));
      }
    }
    return out;
  }

  private static String unquote(String value) {
    if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }
}
