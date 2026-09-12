package io.convalesce.emit.spark;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * Turns a Spark listener event into JSON, using Spark's own serialiser.
 *
 * <p>This is what makes the plugin thin. Spark already knows how to render its events, and {@code
 * JsonProtocol} is what writes the event log, so nothing here walks an object graph or reads a
 * field. A listener that mapped events into some other shape would be far larger and would have to
 * be upgraded in the cluster whenever that shape changed.
 *
 * <p>The method to call moved between releases, so it is resolved once by reflection:
 *
 * <ul>
 *   <li>Spark 3.4 and later: {@code sparkEventToJsonString(SparkListenerEvent)} returns a String.
 *   <li>Spark 3.0 to 3.3: {@code sparkEventToJson(SparkListenerEvent)} returns a json4s {@code
 *       JValue}, which {@code JsonMethods.compact(render(v))} turns into a String.
 * </ul>
 *
 * <p>Reflection rather than a compile-time branch keeps json4s off the compile classpath entirely,
 * so one dependency-free jar covers Spark 3.0 through 4.x.
 *
 * <p>The two string helpers at the bottom are the one place this class touches an event's content,
 * and they touch only the application id: Spark puts it on the application-start event and in a
 * job's properties and nowhere else, so every other event had nothing to say which driver it came
 * from. Reading and inserting one field by text keeps this class free of a JSON parser, which is
 * the whole point of the jar having no dependencies.
 */
final class SparkEventJson {

  private static final Logger LOG = Logger.getLogger(SparkEventJson.class.getName());

  private static final String JSON_PROTOCOL = "org.apache.spark.util.JsonProtocol";
  private static final String JSON_METHODS = "org.json4s.jackson.JsonMethods$";
  private static final String JVALUE_CLASS = "org.json4s.JsonAST$JValue";
  private static final String EVENT_CLASS = "org.apache.spark.scheduler.SparkListenerEvent";

  // What Spark calls the application id on the application-start event, and in the properties a
  // job start carries.
  private static final String APP_ID_FIELD = "\"App ID\"";
  private static final String APP_ID_PROPERTY = "\"spark.app.id\"";

  private static final Strategy STRATEGY = resolve();

  private SparkEventJson() {}

  /**
   * Renders one event.
   *
   * @param event a Spark listener event
   * @return the event as JSON, or null when this Spark cannot be asked
   */
  static String toJson(Object event) {
    if (STRATEGY == null) {
      return null;
    }
    try {
      return STRATEGY.render(event);
    } catch (Throwable t) {
      // A single unserialisable event must not stop the ones after it.
      LOG.fine("convalesce: could not serialise " + event.getClass().getSimpleName());
      return null;
    }
  }

  /** True when this Spark exposes a serialiser we can use. */
  static boolean available() {
    return STRATEGY != null;
  }

  /**
   * Reads the application id out of an event that names one.
   *
   * @param json the event as Spark rendered it
   * @return the id, or null when this event does not name one
   */
  static String readAppId(String json) {
    if (json == null) {
      return null;
    }
    String found = valueAfter(json, APP_ID_FIELD);
    return found != null ? found : valueAfter(json, APP_ID_PROPERTY);
  }

  /**
   * Stamps the application id onto an event that does not name one.
   *
   * <p>Inserted as the first field of the object, which is valid JSON wherever the payload is an
   * object, and left alone when the event already names one or when there is nothing to add.
   *
   * @param json the event as Spark rendered it
   * @param appId the application to name, which may be null
   * @return the event, naming its application
   */
  static String withAppId(String json, String appId) {
    if (json == null || appId == null || json.length() < 2) {
      return json;
    }
    int start = json.indexOf('{');
    if (start < 0 || json.contains(APP_ID_FIELD)) {
      return json;
    }
    String field = APP_ID_FIELD + ":" + quote(appId);
    boolean empty = json.substring(start + 1).trim().startsWith("}");
    return json.substring(0, start + 1) + field + (empty ? "" : ",") + json.substring(start + 1);
  }

  /**
   * The string value that follows a field name.
   *
   * @param json the document to read
   * @param field the quoted field name to look for
   * @return the value, or null when the field is absent or not a string
   */
  private static String valueAfter(String json, String field) {
    int at = json.indexOf(field);
    if (at < 0) {
      return null;
    }
    int colon = json.indexOf(':', at + field.length());
    if (colon < 0) {
      return null;
    }
    int open = json.indexOf('"', colon + 1);
    if (open < 0) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    for (int i = open + 1; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '\\' && i + 1 < json.length()) {
        out.append(json.charAt(++i));
        continue;
      }
      if (c == '"') {
        return out.toString();
      }
      out.append(c);
    }
    return null;
  }

  private static String quote(String value) {
    StringBuilder out = new StringBuilder(value.length() + 2);
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '"' || c == '\\') {
        out.append('\\');
      }
      out.append(c);
    }
    out.append('"');
    return out.toString();
  }

  private interface Strategy {
    String render(Object event) throws Exception;
  }

  private static Strategy resolve() {
    ClassLoader loader = SparkEventJson.class.getClassLoader();
    Class<?> protocol;
    Class<?> eventClass;
    try {
      protocol = Class.forName(JSON_PROTOCOL, true, loader);
      eventClass = Class.forName(EVENT_CLASS, true, loader);
    } catch (Throwable t) {
      LOG.warning("convalesce: Spark's JsonProtocol is not on the classpath; not emitting");
      return null;
    }
    Strategy modern = resolveModern(protocol, eventClass);
    if (modern != null) {
      return modern;
    }
    Strategy legacy = resolveLegacy(protocol, eventClass, loader);
    if (legacy != null) {
      return legacy;
    }
    LOG.warning(
        "convalesce: no known JsonProtocol method on this Spark ("
            + protocol.getPackage()
            + "); not emitting");
    return null;
  }

  /** Spark 3.4+: the serialiser returns a String directly. */
  private static Strategy resolveModern(Class<?> protocol, Class<?> eventClass) {
    try {
      final Method method = protocol.getMethod("sparkEventToJsonString", eventClass);
      return new Strategy() {
        @Override
        public String render(Object event) throws Exception {
          return (String) method.invoke(null, event);
        }
      };
    } catch (Throwable t) {
      return null;
    }
  }

  /**
   * Spark 3.0 to 3.3: the serialiser returns a json4s JValue.
   *
   * <p>The json4s idiom is {@code compact(render(v))}, and that is what an earlier version of this
   * did. It returned null on every event: {@code render} takes an implicit {@code Formats} that
   * reflection sees as a real second parameter, and passing null for it throws. Verified against
   * Spark 3.3.4, {@code compact} has a {@code JValue} overload, so {@code render} is not needed at
   * all -- the serialiser's output goes straight to it.
   */
  private static Strategy resolveLegacy(
      Class<?> protocol, Class<?> eventClass, ClassLoader loader) {
    try {
      final Method toJson = protocol.getMethod("sparkEventToJson", eventClass);
      Class<?> methods = Class.forName(JSON_METHODS, true, loader);
      Class<?> jvalue = Class.forName(JVALUE_CLASS, true, loader);
      // json4s exposes these on a Scala object; MODULE$ is its singleton instance.
      final Object module = methods.getField("MODULE$").get(null);
      // The JValue overload, not the Object one: the latter is a bridge method that dispatches
      // on runtime type and is not guaranteed to accept what we hand it.
      final Method compact = methods.getMethod("compact", jvalue);
      return new Strategy() {
        @Override
        public String render(Object event) throws Exception {
          Object rendered = toJson.invoke(null, event);
          return (String) compact.invoke(module, rendered);
        }
      };
    } catch (Throwable t) {
      return null;
    }
  }
}
