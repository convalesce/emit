package io.convalesce.emit.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
 */
final class Redaction {

  /** Spark's default {@code spark.redaction.regex}. */
  static final String DEFAULT_REGEX = "(?i)secret|password|token|access[.]key";

  /** What Spark writes in place of a redacted value. */
  static final String REPLACEMENT = "*********(redacted)";

  static final String REGEX_KEY = "spark.redaction.regex";

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

  private final Pattern pattern;

  private Redaction(Pattern pattern) {
    this.pattern = pattern;
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
      out = redactMaps(out, field);
    }
    return out;
  }

  private String redactMaps(String json, String field) {
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
      int end = redactMap(json, open, map, changed);
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
   * @return the index just past the object, or -1 when it is not a flat map of strings
   */
  private int redactMap(String json, int open, StringBuilder out, boolean[] changed) {
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
      if (valueEnd < 0) {
        return -1;
      }
      String value = json.substring(valueStart, valueEnd);
      out.append(key).append(':');
      if (pattern.matcher(unquote(key)).find() || pattern.matcher(unquote(value)).find()) {
        out.append('"').append(REPLACEMENT).append('"');
        changed[0] = true;
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
