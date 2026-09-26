package io.convalesce.emit.spark;

import io.convalesce.emit.Emitter;

/**
 * The one emitter a Spark driver sends through.
 *
 * <p>Two things in a driver forward to Convalesce: the listener, with Spark's own events, and the
 * OpenLineage transport, with what OpenLineage computed from the plan. Spark builds each of them
 * itself, so neither can be handed the other's emitter. Sharing one here means both read the same
 * key, endpoint and limits, their observations batch together, and the listener's flush at
 * application end sends whatever the transport queued too.
 */
final class SharedEmitter {

  private SharedEmitter() {}

  /** The driver's emitter, built from the environment the first time anything asks. */
  static Emitter emitter() {
    return Holder.EMITTER;
  }

  /** The running Spark's version, or null when it cannot be read. */
  static String sparkVersion() {
    return Holder.SPARK_VERSION;
  }

  // Initialised on first use, which the JVM makes thread-safe without a lock of ours.
  private static final class Holder {
    static final Emitter EMITTER = new Emitter();
    static final String SPARK_VERSION = readSparkVersion();
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
