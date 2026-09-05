package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.junit.Test;

/**
 * Exercises the Spark 3.3 serialiser against real Spark 3.3 jars.
 *
 * <p>This runs in its own source set because it needs a Spark older than the one the module
 * compiles against, and the two cannot share a classpath.
 *
 * <p>It exists because the first version of the legacy path returned null on every event. The
 * json4s idiom is {@code compact(render(v))}, but {@code render} takes an implicit {@code Formats}
 * that reflection sees as a real parameter, so the call threw and the failure was swallowed.
 * Nothing short of running it against real json4s would have shown that.
 */
public class LegacyJsonProtocolTest {

  @Test
  public void thisSparkReallyIsTheLegacyShape() {
    // If this ever fails, the test is pinned to the wrong Spark and proves nothing.
    boolean hasStringMethod = false;
    for (Method method : org.apache.spark.util.JsonProtocol.class.getMethods()) {
      if (method.getName().equals("sparkEventToJsonString")) {
        hasStringMethod = true;
      }
    }
    assertFalse("expected a Spark whose JsonProtocol returns JValue", hasStringMethod);
  }

  @Test
  public void serialiserIsResolvedOnThisSpark() {
    assertTrue(SparkEventJson.available());
  }

  @Test
  public void rendersAnApplicationEnd() {
    String json = SparkEventJson.toJson(new SparkListenerApplicationEnd(1234L));
    assertNotNull("the legacy path returned nothing", json);
    assertEquals("{\"Event\":\"SparkListenerApplicationEnd\",\"Timestamp\":1234}", json);
  }

  @Test
  public void rendersAJobEndWithItsResult() {
    String json = SparkEventJson.toJson(new SparkListenerJobEnd(7, 99L, JobSucceeded$.MODULE$));
    assertNotNull(json);
    assertTrue(json.contains("\"Event\":\"SparkListenerJobEnd\""));
    assertTrue(json.contains("\"Job ID\":7"));
    assertTrue(json.contains("JobSucceeded"));
  }
}
