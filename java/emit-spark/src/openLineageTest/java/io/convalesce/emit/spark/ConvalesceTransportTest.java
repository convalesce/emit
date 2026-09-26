package io.convalesce.emit.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.convalesce.emit.Config;
import io.convalesce.emit.Emitter;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives the OpenLineage transport against a real HTTP server, with no Spark on the classpath.
 *
 * <p>The events are built with OpenLineage's own builders, so what is serialised is what
 * openlineage-spark would hand the transport.
 */
public class ConvalesceTransportTest {

  private static final URI PRODUCER = URI.create("https://github.com/OpenLineage/OpenLineage");

  private HttpServer server;
  private String url;
  private final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());

  @Before
  public void startServer() throws Exception {
    bodies.clear();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws java.io.IOException {
            byte[] raw = readBytes(exchange.getRequestBody());
            bodies.add(new String(gunzip(raw), "UTF-8"));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
          }
        });
    server.start();
    url = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  @Test
  public void openLineageFindsTheTransportByItsName() {
    TransportBuilder found = null;
    for (TransportBuilder builder : ServiceLoader.load(TransportBuilder.class)) {
      if ("convalesce".equals(builder.getType())) {
        found = builder;
      }
    }
    assertNotNull("no TransportBuilder of type convalesce was registered", found);
    Transport transport = found.build(found.getConfig());
    assertTrue(transport instanceof ConvalesceTransport);
  }

  @Test
  public void aRunEventIsSentAsAnOpenLineageObservation() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 50, 0));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, "3.5.3");
    transport.emit(runEvent(OpenLineage.RunEvent.EventType.COMPLETE, null));
    // A run ending flushes without being asked, since the driver may exit next.
    assertEquals(1, bodies.size());
    String body = bodies.get(0);
    assertTrue(body, body.contains("\"tool\":\"spark\""));
    assertTrue(body, body.contains("\"event\":\"openlineage\""));
    assertTrue(body, body.contains("\"tool_version\":\"3.5.3\""));
    assertTrue(body, body.contains("\"payload\":{\"run_event\":{"));
    assertTrue(body, body.contains("\"eventType\":\"COMPLETE\""));
    assertTrue(body, body.contains("\"namespace\":\"spark_ns\""));
    assertTrue(body, body.contains("\"name\":\"orders.daily\""));
  }

  @Test
  public void aRunStartingIsQueuedUntilSomethingFlushes() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 50, 0));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, null);
    transport.emit(runEvent(OpenLineage.RunEvent.EventType.START, null));
    assertEquals(0, bodies.size());
    transport.close();
    assertEquals(1, bodies.size());
  }

  @Test
  public void anOversizedEventLosesItsLogicalPlanAndNothingElse() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 50, 0, 6000));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, null);
    OpenLineage.RunEvent event =
        runEvent(OpenLineage.RunEvent.EventType.COMPLETE, repeat("Project [id#1]\n", 1000));
    transport.emit(event);
    assertEquals(1, bodies.size());
    String body = bodies.get(0);
    assertFalse(body, body.contains("spark.logicalPlan"));
    assertTrue(body, body.contains("\"name\":\"orders.daily\""));
    assertTrue(body, body.contains("\"marker\""));
    // The event is left as OpenLineage built it, for any transport after this one.
    assertTrue(
        event
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .containsKey(ConvalesceTransport.LOGICAL_PLAN));
  }

  @Test
  public void anEventWithinTheLimitKeepsItsLogicalPlan() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 50, 0));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, null);
    transport.emit(runEvent(OpenLineage.RunEvent.EventType.COMPLETE, "Project [id#1]"));
    assertTrue(bodies.get(0), bodies.get(0).contains("\"spark.logicalPlan\""));
  }

  @Test
  public void datasetAndJobEventsAreNotForwarded() {
    Emitter emitter = new Emitter(Config.of(url, "secret-key", 1, 0));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, null);
    OpenLineage ol = new OpenLineage(PRODUCER);
    transport.emit(
        ol.newDatasetEventBuilder()
            .eventTime(ZonedDateTime.now())
            .dataset(ol.newStaticDatasetBuilder().namespace("ns").name("t").build())
            .build());
    transport.emit(
        ol.newJobEventBuilder()
            .eventTime(ZonedDateTime.now())
            .job(ol.newJobBuilder().namespace("ns").name("j").build())
            .build());
    transport.close();
    assertEquals(0, bodies.size());
  }

  @Test
  public void anUnreachableEndpointNeverReachesTheCaller() {
    Emitter emitter = new Emitter(Config.of("http://127.0.0.1:1", "secret-key", 1, 0));
    ConvalesceTransport transport = new ConvalesceTransport(emitter, null);
    transport.emit(runEvent(OpenLineage.RunEvent.EventType.FAIL, null));
    transport.close();
  }

  private static OpenLineage.RunEvent runEvent(OpenLineage.RunEvent.EventType type, String plan) {
    OpenLineage ol = new OpenLineage(PRODUCER);
    OpenLineage.RunFacetsBuilder facets = ol.newRunFacetsBuilder();
    OpenLineage.RunFacet marker = ol.newRunFacet();
    marker.getAdditionalProperties().put("kept", "yes");
    facets.put("marker", marker);
    if (plan != null) {
      OpenLineage.RunFacet logical = ol.newRunFacet();
      logical.getAdditionalProperties().put("plan", plan);
      facets.put(ConvalesceTransport.LOGICAL_PLAN, logical);
    }
    return ol.newRunEventBuilder()
        .eventType(type)
        .eventTime(ZonedDateTime.now())
        .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(facets.build()).build())
        .job(ol.newJobBuilder().namespace("spark_ns").name("orders.daily").build())
        .inputs(Collections.<OpenLineage.InputDataset>emptyList())
        .outputs(Collections.<OpenLineage.OutputDataset>emptyList())
        .build();
  }

  private static String repeat(String unit, int times) {
    StringBuilder out = new StringBuilder(unit.length() * times);
    for (int i = 0; i < times; i++) {
      out.append(unit);
    }
    return out.toString();
  }

  private static byte[] readBytes(InputStream in) throws java.io.IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(byte[] data) throws java.io.IOException {
    return readBytes(new GZIPInputStream(new java.io.ByteArrayInputStream(data)));
  }
}
