package io.convalesce.emit.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Which of Spark's events are worth forwarding.
 *
 * <p>A run is described by the application, the jobs and the SQL executions starting and ending.
 * Everything else Spark announces describes the inside of a job: one event per task, about 190
 * values each, plus a stage completion per stage and an adaptive plan update per replan. A job with
 * ten thousand tasks was ten thousand observations, none of which a receiver read, sent from the
 * customer's driver.
 *
 * <p>So the default is the run, and the rest is opt-in through {@code CONVALESCE_SPARK_EVENTS}:
 * {@code all} for everything Spark offers, or a comma-separated list of event class simple names to
 * add to the default set. An unset or empty value leaves the default.
 */
final class SparkEvents {

  /** What describes a run, and nothing else. */
  private static final Set<String> RUN_EVENTS =
      Collections.unmodifiableSet(
          new HashSet<String>(
              Arrays.asList(
                  "SparkListenerApplicationStart",
                  "SparkListenerApplicationEnd",
                  "SparkListenerJobStart",
                  "SparkListenerJobEnd",
                  "SparkListenerSQLExecutionStart",
                  "SparkListenerSQLExecutionEnd")));

  private static final String VARIABLE = "CONVALESCE_SPARK_EVENTS";
  private static final String ALL = "all";

  private final Set<String> wanted;
  private final boolean everything;

  private SparkEvents(Set<String> wanted, boolean everything) {
    this.wanted = wanted;
    this.everything = everything;
  }

  /**
   * Reads the choice from the process environment.
   *
   * @return what to forward
   */
  static SparkEvents fromEnvironment() {
    return of(System.getenv(VARIABLE));
  }

  /**
   * Builds the choice from one setting, for tests and embedders.
   *
   * @param setting {@code all}, a comma-separated list of event names to add, or null
   * @return what to forward
   */
  static SparkEvents of(String setting) {
    String value = setting == null ? "" : setting.trim();
    if (value.isEmpty()) {
      return new SparkEvents(RUN_EVENTS, false);
    }
    if (value.toLowerCase(Locale.ROOT).equals(ALL)) {
      return new SparkEvents(RUN_EVENTS, true);
    }
    Set<String> named = new HashSet<String>(RUN_EVENTS);
    for (String each : value.split(",")) {
      String name = each.trim();
      if (!name.isEmpty()) {
        named.add(name);
      }
    }
    return new SparkEvents(Collections.unmodifiableSet(named), false);
  }

  /**
   * Whether one event is worth forwarding.
   *
   * @param simpleName the event class's simple name, as the envelope carries it
   * @return whether to send it
   */
  boolean wanted(String simpleName) {
    return everything || wanted.contains(simpleName);
  }
}
