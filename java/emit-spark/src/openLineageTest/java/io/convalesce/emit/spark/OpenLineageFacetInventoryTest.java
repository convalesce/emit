package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.openlineage.client.OpenLineage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.Test;

/**
 * Every OpenLineage facet the libraries on this classpath can put in a run event, and whether
 * collect decodes it.
 *
 * <p>{@code openlineage-facets.yml} is the list, shared with collect, whose own test checks the
 * {@code decoded} half against its decoder. This one checks the list is complete: a newer
 * openlineage-java with a new standard facet, or a newer openlineage-spark with a new facet class,
 * fails here until somebody decides whether collect should read it.
 */
public class OpenLineageFacetInventoryTest {

  private static final String LIST = "/openlineage-facets.yml";
  private static final String SPARK_SECTION = "openlineage_spark";
  private static final String DECODED = "decoded";
  private static final String IGNORED = "ignored:";

  /** Where each standard facet container sits in a RunEvent, as the list's sections name it. */
  private static final Map<String, Class<?>> CONTAINERS = new TreeMap<String, Class<?>>();

  static {
    CONTAINERS.put("run", OpenLineage.RunFacets.class);
    CONTAINERS.put("job", OpenLineage.JobFacets.class);
    CONTAINERS.put("dataset", OpenLineage.DatasetFacets.class);
    CONTAINERS.put("input", OpenLineage.InputDatasetInputFacets.class);
    CONTAINERS.put("output", OpenLineage.OutputDatasetOutputFacets.class);
  }

  @Test
  public void everyStandardFacetIsDecodedOrIgnoredWithAReason() throws IOException {
    Map<String, Map<String, String>> list = read();
    List<String> missing = new ArrayList<String>();
    for (Map.Entry<String, Class<?>> container : CONTAINERS.entrySet()) {
      Map<String, String> section = list.get(container.getKey());
      assertTrue("no `" + container.getKey() + ":` section", section != null);
      for (String facet : facetsOf(container.getValue())) {
        if (!section.containsKey(facet)) {
          missing.add(container.getKey() + "." + facet);
        }
      }
    }
    if (!missing.isEmpty()) {
      fail(
          "openlineage-java defines facets openlineage-facets.yml does not classify; mark each "
              + "`decoded` (and read it in collect) or `ignored: <why>`:\n  "
              + String.join("\n  ", missing));
    }
  }

  @Test
  public void everyOpenLineageSparkFacetClassIsListed() throws IOException {
    String jarPath = System.getProperty("openlineage.spark.jar");
    assertTrue("openlineage.spark.jar is not set", jarPath != null && !jarPath.isEmpty());
    Map<String, Map<String, String>> list = read();
    Map<String, String> classes = list.get(SPARK_SECTION);
    assertTrue("no `" + SPARK_SECTION + ":` section", classes != null);
    TreeSet<String> shipped = new TreeSet<String>();
    try (JarFile jar = new JarFile(new File(jarPath))) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        String path = entries.nextElement().getName();
        if (path.startsWith("io/openlineage/spark")
            && path.contains("/facets/")
            && path.endsWith("Facet.class")
            && !path.contains("$")) {
          shipped.add(path.substring(path.lastIndexOf('/') + 1, path.length() - ".class".length()));
        }
      }
    }
    assertTrue("found no facet classes in " + jarPath, shipped.contains("SparkPropertyFacet"));
    TreeSet<String> unlisted = new TreeSet<String>(shipped);
    unlisted.removeAll(classes.keySet());
    assertTrue(
        "openlineage-spark ships facet classes openlineage-facets.yml does not map to a facet: "
            + unlisted,
        unlisted.isEmpty());
    TreeSet<String> gone = new TreeSet<String>(classes.keySet());
    gone.removeAll(shipped);
    assertTrue(
        "openlineage-facets.yml maps classes this openlineage-spark lacks: " + gone,
        gone.isEmpty());
    for (Map.Entry<String, String> each : classes.entrySet()) {
      String target = each.getValue();
      int dot = target.indexOf('.');
      assertTrue(each.getKey() + " maps to `" + target + "`, not `section.facet`", dot > 0);
      Map<String, String> section = list.get(target.substring(0, dot));
      assertTrue(
          each.getKey() + " is sent as " + target + ", which is not classified",
          section != null && section.containsKey(target.substring(dot + 1)));
    }
  }

  @Test
  public void everyListedFacetExistsAndHasAVerdict() throws IOException {
    Map<String, Map<String, String>> list = read();
    TreeSet<String> known = new TreeSet<String>();
    for (Map.Entry<String, Class<?>> container : CONTAINERS.entrySet()) {
      for (String facet : facetsOf(container.getValue())) {
        known.add(container.getKey() + "." + facet);
      }
    }
    known.addAll(list.get(SPARK_SECTION).values());
    List<String> problems = new ArrayList<String>();
    for (String section : CONTAINERS.keySet()) {
      for (Map.Entry<String, String> facet : list.get(section).entrySet()) {
        String name = section + "." + facet.getKey();
        String verdict = facet.getValue();
        if (!known.contains(name)) {
          problems.add(name + " is defined by neither openlineage-java nor openlineage-spark");
        }
        boolean ignoredWithReason =
            verdict.startsWith(IGNORED) && verdict.length() > IGNORED.length() + 1;
        if (!verdict.equals(DECODED) && !ignoredWithReason) {
          problems.add(name + " is `" + verdict + "`, not `decoded` or `ignored: <why>`");
        }
      }
    }
    assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /** The facet names a container's getters expose, as they are serialised. */
  private static TreeSet<String> facetsOf(Class<?> container) {
    TreeSet<String> out = new TreeSet<String>();
    for (Method method : container.getDeclaredMethods()) {
      String name = method.getName();
      if (!name.startsWith("get")
          || name.equals("getAdditionalProperties")
          || method.getParameterTypes().length != 0
          || method.isSynthetic()) {
        continue;
      }
      String property = jsonProperty(method);
      out.add(
          property != null ? property : Character.toLowerCase(name.charAt(3)) + name.substring(4));
    }
    assertTrue("found no facets on " + container.getName(), !out.isEmpty());
    return out;
  }

  /** A {@code @JsonProperty} name on the getter, read without compiling against Jackson. */
  private static String jsonProperty(Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (annotation.annotationType().getSimpleName().equals("JsonProperty")) {
        try {
          Object value = annotation.annotationType().getMethod("value").invoke(annotation);
          if (value instanceof String && !((String) value).isEmpty()) {
            return (String) value;
          }
        } catch (ReflectiveOperationException e) {
          return null;
        }
      }
    }
    return null;
  }

  /**
   * The list, as section to facet to verdict.
   *
   * <p>Two levels of plain {@code key: value} YAML, read by hand so the test needs no YAML library;
   * collect reads the same file with a real one.
   */
  static Map<String, Map<String, String>> read() throws IOException {
    Map<String, Map<String, String>> out = new TreeMap<String, Map<String, String>>();
    InputStream in = OpenLineageFacetInventoryTest.class.getResourceAsStream(LIST);
    assertTrue(LIST + " is missing", in != null);
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      Map<String, String> section = null;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty() || line.trim().startsWith("#")) {
          continue;
        }
        if (!line.startsWith(" ")) {
          section = new TreeMap<String, String>();
          out.put(line.trim().replaceAll(":$", ""), section);
          continue;
        }
        assertTrue("an entry before any section: " + line, section != null);
        int colon = line.indexOf(':');
        assertTrue("not `key: value`: " + line, colon > 0);
        String key = line.substring(0, colon).trim();
        String value = line.substring(colon + 1).trim();
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length() - 1);
        }
        assertEquals(key + " is listed twice", null, section.put(key, value));
      }
    }
    return out;
  }
}
