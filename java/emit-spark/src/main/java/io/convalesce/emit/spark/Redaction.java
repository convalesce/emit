package io.convalesce.emit.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hides credentials in the configuration an event carries, the way Spark's event log does.
 *
 * <p>A job start carries the whole Spark configuration as its {@code Properties}: every {@code
 * spark.hadoop.fs.s3a.secret.key}, every password passed with {@code --conf}. Spark's event log
 * redacts them before writing, but a listener is handed the event as it is, so forwarding it
 * unchanged sent the customer's secrets off their cluster.
 *
 * <p>The rule is Spark's own ({@code Utils.redact}): a value is replaced when its key or the value
 * itself matches {@code spark.redaction.regex}, so a JDBC URL with a password in it goes too. It is
 * applied only inside the maps that hold configuration, where Spark applies it, and not to a job's
 * description or a stage's name, which can say "token" without holding one.
 *
 * <p>Two things go further than Spark's rule, because a job's own rule knows nothing of them. A
 * setting named for Convalesce's ingest key ({@code spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY})
 * is a secret whatever the job's {@code spark.redaction.regex} says. And a value that is itself a
 * list of {@code NAME=value} entries, as the parameter AWS Glue hands a job its environment in, has
 * the value of each secret-named entry masked and the rest kept.
 */
final class Redaction {

  /** Spark's default {@code spark.redaction.regex}. */
  static final String DEFAULT_REGEX = "(?i)secret|password|token|access[.]key";

  /** What Spark writes in place of a redacted value. */
  static final String REPLACEMENT = "*********(redacted)";

  static final String REGEX_KEY = "spark.redaction.regex";

  /** The names Convalesce's own key is set under, secret under any rule a job configures. */
  static final String OWN_KEYS_REGEX = "(?i)ingest[._]?key";

  /** What replaces the value of one secret-named entry inside a value that lists several. */
  static final String ENTRY_MASK = "***";

  private static final Pattern OWN_KEYS = Pattern.compile(OWN_KEYS_REGEX);

  // One `NAME=value` entry inside a value: a name that starts where another could not be running
  // on, and a value that runs to the next separator. The value is matched as it is written in the
  // JSON, so it also ends at a backslash or a quote, where an escape starts.
  private static final Pattern ENTRY =
      Pattern.compile("(?<![A-Za-z0-9_.-])([A-Za-z_][A-Za-z0-9_.-]*)=([^,;&\\s\\\\\"]+)");

  // The event fields whose value is a configuration map: a job's and a stage's properties, a SQL
  // execution's changed settings, and an environment update's sections.
  private static final List<String> CONFIG_MAPS =
      Collections.unmodifiableList(
          Arrays.asList(
              "\"Properties\"",
              "\"modifiedConfigs\"",
              "\"Spark Properties\"",
              "\"Hadoop Properties\"",
              "\"System Properties\"",
              "\"Metrics Properties\""));

  // The same, in an OpenLineage run event: the Spark settings a job asked to have captured (the
  // `spark_properties` facet) and what OpenLineage read of the platform (`environment-properties`).
  private static final List<String> RUN_EVENT_MAPS =
      Collections.unmodifiableList(Arrays.asList("\"properties\"", "\"environment-properties\""));

  private final Pattern pattern;

  private Redaction(Pattern pattern) {
    this.pattern = pattern;
  }

  /**
   * The rule of the Spark this driver is running, for a caller Spark did not hand a configuration.
   *
   * @return the job's rule, or Spark's default when there is no running Spark to ask
   */
  static Redaction ofRunningSpark() {
    String regex = null;
    try {
      org.apache.spark.SparkEnv env = org.apache.spark.SparkEnv.get();
      regex = env == null ? null : env.conf().get(REGEX_KEY, null);
    } catch (Throwable t) {
      // No Spark on the classpath, or none started: the default rule still applies.
    }
    return of(regex);
  }

  /**
   * The rule a job configured, or Spark's default when it configured none or one that does not
   * compile.
   *
   * @param regex the job's {@code spark.redaction.regex}, which may be null
   * @return the rule
   */
  static Redaction of(String regex) {
    if (regex != null && !regex.trim().isEmpty()) {
      try {
        return new Redaction(Pattern.compile(regex));
      } catch (RuntimeException e) {
        // A job that cannot compile its own rule still gets Spark's.
      }
    }
    return new Redaction(Pattern.compile(DEFAULT_REGEX));
  }

  /**
   * Redacts every configuration map in one event.
   *
   * @param json the event as Spark rendered it
   * @return the event, with sensitive values replaced
   */
  String apply(String json) {
    if (json == null) {
      return null;
    }
    String out = json;
    for (String field : CONFIG_MAPS) {
      out = redactMaps(out, field, false);
    }
    return out;
  }

  /**
   * Redacts the configuration an OpenLineage run event carries.
   *
   * <p>OpenLineage does not redact what it captures. Unlike a Spark event's maps, these can hold
   * values that are not strings (a platform's mount points, say). A map inside one is redacted by
   * the same rule; anything else is kept as it is unless its key names a credential.
   *
   * @param json the run event as OpenLineage rendered it
   * @return the event, with sensitive values replaced
   */
  String applyToRunEvent(String json) {
    if (json == null) {
      return null;
    }
    String out = json;
    for (String field : RUN_EVENT_MAPS) {
      out = redactMaps(out, field, true);
    }
    return out;
  }

  private String redactMaps(String json, String field, boolean anyValue) {
    StringBuilder out = null;
    int copied = 0;
    int from = 0;
    while (true) {
      int at = json.indexOf(field, from);
      if (at < 0) {
        break;
      }
      int open = skipSpace(json, at + field.length());
      if (open >= json.length() || json.charAt(open) != ':') {
        from = at + field.length();
        continue;
      }
      open = skipSpace(json, open + 1);
      if (open >= json.length() || json.charAt(open) != '{') {
        from = at + field.length();
        continue;
      }
      StringBuilder map = new StringBuilder();
      boolean[] changed = new boolean[1];
      int end = redactMap(json, open, map, changed, anyValue);
      if (end < 0 || !changed[0]) {
        // Nothing to hide, or not a flat map of strings, which is left as it is rather than
        // guessed at.
        from = end < 0 ? open + 1 : end;
        continue;
      }
      if (out == null) {
        out = new StringBuilder(json.length());
      }
      out.append(json, copied, open).append(map);
      copied = end;
      from = end;
    }
    if (out == null) {
      return json;
    }
    return out.append(json, copied, json.length()).toString();
  }

  /**
   * Copies one flat {@code {"key":"value",...}} object into {@code out}, redacted.
   *
   * @param changed set when a value was replaced
   * @param anyValue whether a value may be something other than a string
   * @return the index just past the object, or -1 when it is not a map this can read
   */
  private int redactMap(
      String json, int open, StringBuilder out, boolean[] changed, boolean anyValue) {
    out.append('{');
    int i = skipSpace(json, open + 1);
    if (i < json.length() && json.charAt(i) == '}') {
      out.append('}');
      return i + 1;
    }
    while (i < json.length()) {
      int keyEnd = stringEnd(json, i);
      if (keyEnd < 0) {
        return -1;
      }
      String key = json.substring(i, keyEnd);
      int colon = skipSpace(json, keyEnd);
      if (colon >= json.length() || json.charAt(colon) != ':') {
        return -1;
      }
      int valueStart = skipSpace(json, colon + 1);
      int valueEnd = stringEnd(json, valueStart);
      boolean text = valueEnd >= 0;
      if (!text && anyValue) {
        valueEnd = valueEnd(json, valueStart);
      }
      if (valueEnd < 0) {
        return -1;
      }
      String value = json.substring(valueStart, valueEnd);
      out.append(key).append(':');
      String entries = null;
      if (secretName(unquote(key)) || (text && pattern.matcher(unquote(value)).find())) {
        out.append('"').append(REPLACEMENT).append('"');
        changed[0] = true;
      } else if (text && (entries = maskEntries(value)) != null) {
        out.append(entries);
        changed[0] = true;
      } else if (!text && value.charAt(0) == '{') {
        // A map inside the map is configuration too, and is held to the same rule.
        if (redactMap(json, valueStart, out, changed, true) < 0) {
          return -1;
        }
      } else {
        out.append(value);
      }
      i = skipSpace(json, valueEnd);
      if (i >= json.length()) {
        return -1;
      }
      char c = json.charAt(i);
      if (c == '}') {
        out.append('}');
        return i + 1;
      }
      if (c != ',') {
        return -1;
      }
      out.append(',');
      i = skipSpace(json, i + 1);
    }
    return -1;
  }

  /** Whether a setting's or an entry's name says it holds a credential. */
  private boolean secretName(String name) {
    return pattern.matcher(name).find() || OWN_KEYS.matcher(name).find();
  }

  /**
   * Masks the secret-named entries of a value that lists {@code NAME=value} entries.
   *
   * @param value a JSON string, quotes included
   * @return the string with those entries' values masked, or null when it has none
   */
  private String maskEntries(String value) {
    if (value.indexOf('=') < 0) {
      return null;
    }
    Matcher entry = ENTRY.matcher(value);
    StringBuffer out = null;
    while (entry.find()) {
      if (!secretName(entry.group(1))) {
        continue;
      }
      if (out == null) {
        out = new StringBuffer(value.length());
      }
      entry.appendReplacement(out, Matcher.quoteReplacement(entry.group(1) + "=" + ENTRY_MASK));
    }
    return out == null ? null : entry.appendTail(out).toString();
  }

  /** The index just past the JSON string starting at {@code start}, or -1 if none starts there. */
  private static int stringEnd(String json, int start) {
    if (start >= json.length() || json.charAt(start) != '"') {
      return -1;
    }
    for (int i = start + 1; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '\\') {
        i++;
      } else if (c == '"') {
        return i + 1;
      }
    }
    return -1;
  }

  /**
   * The index just past the JSON value starting at {@code start} that is not a string: a nested
   * object or array, a number or a literal. -1 when it never ends.
   */
  private static int valueEnd(String json, int start) {
    int depth = 0;
    for (int i = start; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '"') {
        i = stringEnd(json, i) - 1;
        if (i < 0) {
          return -1;
        }
      } else if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        if (depth == 0) {
          return i > start ? i : -1;
        }
        depth--;
        if (depth == 0) {
          return i + 1;
        }
      } else if (depth == 0 && (c == ',' || Character.isWhitespace(c))) {
        return i > start ? i : -1;
      }
    }
    return -1;
  }

  private static String unquote(String quoted) {
    // Escapes are left in: the rule is matched against words, and an escape never makes one.
    return quoted.substring(1, quoted.length() - 1);
  }

  private static int skipSpace(String json, int i) {
    while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
      i++;
    }
    return i;
  }
}
