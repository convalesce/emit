package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Which events are forwarded, and how an event learns which application it came from. */
public class SparkEventsTest {

  @Test
  public void theRunIsForwardedAndTheInsideOfAJobIsNot() {
    SparkEvents events = SparkEvents.of(null);
    assertTrue(events.wanted("SparkListenerApplicationStart"));
    assertTrue(events.wanted("SparkListenerJobEnd"));
    assertTrue(events.wanted("SparkListenerSQLExecutionStart"));
    // One per task, about 190 values each, and no receiver reads them.
    assertFalse(events.wanted("SparkListenerTaskEnd"));
    assertFalse(events.wanted("SparkListenerStageCompleted"));
    assertFalse(events.wanted("SparkListenerSQLAdaptiveExecutionUpdate"));
  }

  @Test
  public void everythingCanBeAskedFor() {
    SparkEvents events = SparkEvents.of("all");
    assertTrue(events.wanted("SparkListenerTaskEnd"));
    assertTrue(events.wanted("SparkListenerBlockManagerAdded"));
  }

  @Test
  public void namedEventsAreAddedToTheDefault() {
    SparkEvents events = SparkEvents.of("SparkListenerTaskEnd, SparkListenerStageCompleted");
    assertTrue(events.wanted("SparkListenerTaskEnd"));
    assertTrue(events.wanted("SparkListenerStageCompleted"));
    // Still the run, and still not everything.
    assertTrue(events.wanted("SparkListenerJobStart"));
    assertFalse(events.wanted("SparkListenerBlockManagerAdded"));
  }

  @Test
  public void anEmptySettingLeavesTheDefault() {
    assertFalse(SparkEvents.of("   ").wanted("SparkListenerTaskEnd"));
    assertTrue(SparkEvents.of("   ").wanted("SparkListenerJobStart"));
  }

  @Test
  public void theApplicationIdIsReadFromWhereverSparkPutIt() {
    String start = "{\"Event\":\"SparkListenerApplicationStart\",\"App ID\":\"local-1789\"}";
    assertEquals("local-1789", SparkEventJson.readAppId(start));
    String jobStart = "{\"Job ID\":0,\"Properties\":{\"spark.app.id\":\"local-1789\"}}";
    assertEquals("local-1789", SparkEventJson.readAppId(jobStart));
    assertNull(SparkEventJson.readAppId("{\"Job ID\":0,\"Completion Time\":1}"));
    assertNull(SparkEventJson.readAppId(null));
  }

  @Test
  public void anEventThatNamesNoApplicationIsStamped() {
    // A job end, an application end and a SQL execution all arrive like this.
    String end = "{\"Event\":\"SparkListenerJobEnd\",\"Job ID\":0}";
    String stamped = SparkEventJson.withAppId(end, "local-1789");
    assertEquals(
        "{\"App ID\":\"local-1789\",\"Event\":\"SparkListenerJobEnd\",\"Job ID\":0}", stamped);
    assertEquals("local-1789", SparkEventJson.readAppId(stamped));
  }

  @Test
  public void anEventThatNamesOneIsLeftAlone() {
    String start = "{\"App ID\":\"local-1789\",\"User\":\"root\"}";
    assertEquals(start, SparkEventJson.withAppId(start, "local-other"));
  }

  @Test
  public void nothingToStampChangesNothing() {
    String end = "{\"Job ID\":0}";
    assertEquals(end, SparkEventJson.withAppId(end, null));
    assertNull(SparkEventJson.withAppId(null, "local-1789"));
    assertEquals("{\"App ID\":\"local-1789\"}", SparkEventJson.withAppId("{}", "local-1789"));
  }
}
