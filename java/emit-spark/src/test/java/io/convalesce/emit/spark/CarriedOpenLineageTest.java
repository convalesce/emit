package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.convalesce.emit.Config;
import io.convalesce.emit.Emitter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.junit.Test;

/** When OpenLineage is started for a job, when it is left alone, and that it sees every event. */
public class CarriedOpenLineageTest {

  private static final Map<String, String> NO_ENV = Collections.<String, String>emptyMap();
  private static final String STAND_IN = StandIn.class.getName();

  /** Stands in for OpenLineage's listener, which this classpath deliberately lacks. */
  public static class StandIn extends SparkListener {
    final SparkConf conf;

    public StandIn(SparkConf conf) {
      this.conf = conf;
    }
  }

  /** A listener whose constructor fails, as one built for another Spark might. */
  public static class Broken extends SparkListener {
    public Broken(SparkConf conf) {
      throw new IllegalStateException("not this Spark");
    }
  }

  @Test
  public void aJobThatSetNothingGetsOpenLineagePointedAtOurTransport() {
    SparkConf conf = new SparkConf(false);
    SparkListenerInterface started = CarriedOpenLineage.start(conf, NO_ENV, STAND_IN);
    assertTrue(started instanceof StandIn);
    // Handed the driver's own configuration, which is where OpenLineage reads its settings.
    assertSame(conf, ((StandIn) started).conf);
    assertEquals("convalesce", conf.get(CarriedOpenLineage.TRANSPORT_TYPE));
    assertEquals("true", conf.get(CarriedOpenLineage.DATASET_LINEAGE));
  }

  @Test
  public void settingsThatChooseNoBackendAreKeptAndDoNotStopIt() {
    SparkConf conf =
        new SparkConf(false)
            .set("spark.openlineage.namespace", "nightly")
            .set("spark.openlineage.parentJobName", "dag.task")
            .set("spark.openlineage.capturedProperties", "spark.master")
            .set(CarriedOpenLineage.DATASET_LINEAGE, "false")
            .set("spark.extraListeners", ConvalesceSparkListener.class.getName());
    assertNotNull(CarriedOpenLineage.start(conf, NO_ENV, STAND_IN));
    assertEquals("nightly", conf.get("spark.openlineage.namespace"));
    assertEquals("false", conf.get(CarriedOpenLineage.DATASET_LINEAGE));
  }

  @Test
  public void aJobsOwnOpenLineageIsLeftExactlyAsItIs() {
    List<SparkConf> theirs = new ArrayList<SparkConf>();
    theirs.add(
        new SparkConf(false)
            .set(
                "spark.extraListeners",
                ConvalesceSparkListener.class.getName() + " , " + CarriedOpenLineage.LISTENER));
    theirs.add(new SparkConf(false).set("spark.openlineage.transport.type", "http"));
    theirs.add(new SparkConf(false).set("spark.openlineage.transport.url", "http://lineage:5000"));
    theirs.add(new SparkConf(false).set("spark.openlineage.transport.topicName", "lineage"));
    theirs.add(new SparkConf(false).set("spark.openlineage.url", "http://lineage:5000"));
    theirs.add(new SparkConf(false).set("spark.openlineage.host", "http://lineage:5000"));
    theirs.add(new SparkConf(false).set("spark.openlineage.disabled", "true"));
    for (SparkConf conf : theirs) {
      String before = conf.toDebugString();
      assertNull(before, CarriedOpenLineage.start(conf, NO_ENV, STAND_IN));
      assertEquals(before, conf.toDebugString());
    }
  }

  @Test
  public void openLineagesOwnEnvironmentIsTheJobsChoiceToo() {
    String[][] theirs = {
      {"OPENLINEAGE_URL", "http://lineage:5000"},
      {"OPENLINEAGE_CONFIG", "/etc/openlineage.yml"},
      {"OPENLINEAGE_DISABLED", "true"},
      {"OPENLINEAGE__TRANSPORT__TYPE", "http"},
    };
    for (String[] each : theirs) {
      SparkConf conf = new SparkConf(false);
      assertNull(each[0], CarriedOpenLineage.start(conf, env(each[0], each[1]), STAND_IN));
      assertFalse(each[0], conf.contains(CarriedOpenLineage.TRANSPORT_TYPE));
    }
    // Set but saying nothing is not a choice.
    assertNotNull(
        CarriedOpenLineage.start(
            new SparkConf(false), env("OPENLINEAGE_DISABLED", "false"), STAND_IN));
  }

  @Test
  public void itCanBeSwitchedOff() {
    for (String off : new String[] {"false", "0", "no", "OFF"}) {
      SparkConf conf = new SparkConf(false);
      assertNull(off, CarriedOpenLineage.start(conf, env("CONVALESCE_OPENLINEAGE", off), STAND_IN));
      assertFalse(off, conf.contains(CarriedOpenLineage.TRANSPORT_TYPE));
    }
    assertNull(
        CarriedOpenLineage.start(
            new SparkConf(false), env("CONVALESCE_ENABLED", "false"), STAND_IN));
    assertNotNull(
        CarriedOpenLineage.start(
            new SparkConf(false), env("CONVALESCE_OPENLINEAGE", "true"), STAND_IN));
  }

  @Test
  public void ourTransportNamedWithoutItsListenerStillStartsIt() {
    SparkConf conf = new SparkConf(false).set(CarriedOpenLineage.TRANSPORT_TYPE, "convalesce");
    assertNotNull(CarriedOpenLineage.start(conf, NO_ENV, STAND_IN));
  }

  @Test
  public void withoutOpenLineageOnTheClasspathNothingChanges() {
    SparkConf conf = new SparkConf(false);
    // The real class name: this test classpath has no openlineage-spark, as a job that added
    // only the unsuffixed jar has none.
    assertNull(CarriedOpenLineage.start(conf));
    assertFalse(conf.contains(CarriedOpenLineage.TRANSPORT_TYPE));
    assertFalse(conf.contains(CarriedOpenLineage.DATASET_LINEAGE));
  }

  @Test
  public void twoListenersOnOneContextStartItOnce() {
    SparkConf conf = new SparkConf(false);
    assertNotNull(CarriedOpenLineage.start(conf, NO_ENV, STAND_IN));
    assertNull(CarriedOpenLineage.start(conf, NO_ENV, STAND_IN));
    // A driver's next context is another configuration, and gets its own.
    assertNotNull(CarriedOpenLineage.start(new SparkConf(false), NO_ENV, STAND_IN));
  }

  @Test
  public void aListenerThatCannotBeBuiltNeverReachesTheJob() {
    assertNull(CarriedOpenLineage.start(new SparkConf(false), NO_ENV, Broken.class.getName()));
    assertNull(CarriedOpenLineage.start(null, NO_ENV, STAND_IN));
  }

  @Test
  public void everyCallbackSparkMakesIsPassedOn() throws Exception {
    List<String> passed = new ArrayList<String>();
    ConvalesceSparkListener listener = listenerCarrying(recording(passed, null));
    TreeSet<String> made = new TreeSet<String>();
    for (Method method : ConvalesceSparkListener.class.getDeclaredMethods()) {
      if (method.getName().startsWith("on") && method.getParameterTypes().length == 1) {
        made.add(method.getName());
        // No event at all: only whether the call is passed on is of interest here.
        method.invoke(listener, new Object[] {null});
      }
    }
    assertTrue(made.toString(), made.contains("onOtherEvent") && made.contains("onJobStart"));
    assertEquals(made, new TreeSet<String>(passed));
  }

  @Test
  public void aFailureInsideOpenLineageNeverReachesTheJob() {
    List<String> calls = new ArrayList<String>();
    ConvalesceSparkListener listener =
        listenerCarrying(recording(calls, new IllegalStateException("openlineage broke")));
    listener.onJobStart(null);
    listener.onOtherEvent(null);
    listener.onApplicationEnd(null);
    // One event it could not read says nothing about the next.
    assertEquals(3, calls.size());
  }

  @Test
  public void anOpenLineageBuiltForAnotherScalaIsAskedOnce() {
    List<String> calls = new ArrayList<String>();
    ConvalesceSparkListener listener =
        listenerCarrying(recording(calls, new NoSuchMethodError("Seq stageIds()")));
    listener.onJobStart(null);
    listener.onJobEnd(null);
    listener.onApplicationEnd(null);
    assertEquals(Collections.singletonList("onJobStart"), calls);
  }

  private static ConvalesceSparkListener listenerCarrying(SparkListenerInterface lineage) {
    Emitter emitter = new Emitter(Config.of("http://127.0.0.1:1", "secret-key", 50, 0));
    return new ConvalesceSparkListener(emitter, "3.5.3", Redaction.of(null), lineage);
  }

  private static SparkListenerInterface recording(
      final List<String> calls, final Throwable failure) {
    return (SparkListenerInterface)
        Proxy.newProxyInstance(
            CarriedOpenLineageTest.class.getClassLoader(),
            new Class<?>[] {SparkListenerInterface.class},
            new InvocationHandler() {
              @Override
              public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                calls.add(method.getName());
                if (failure != null) {
                  throw failure;
                }
                return null;
              }
            });
  }

  private static Map<String, String> env(String name, String value) {
    Map<String, String> env = new HashMap<String, String>();
    env.put(name, value);
    return env;
  }
}
