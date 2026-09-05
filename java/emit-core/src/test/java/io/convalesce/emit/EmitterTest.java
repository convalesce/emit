package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives the emitter against a real HTTP server.
 *
 * <p>What leaves the process is the claim this package makes, so a stub at that boundary would test
 * nothing.
 */
public class EmitterTest {

  private HttpServer server;
  private String url;
  private final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());
  private final List<String> auth = Collections.synchronizedList(new ArrayList<String>());
  private volatile int status = 200;

  @Before
  public void startServer() throws Exception {
    bodies.clear();
    auth.clear();
    status = 200;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws java.io.IOException {
            bodies.add(read(exchange.getRequestBody()));
            auth.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            exchange.sendResponseHeaders(status, -1);
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

  private Emitter emitter(int batchSize, int maxRetries) {
    return new Emitter(Config.of(url, "secret-key", "acme", batchSize, maxRetries));
  }

  @Test
  public void payloadCrossesVerbatim() {
    // The whole claim: the tool's own JSON is embedded, not re-encoded.
    String payload = "{\"dag\":\"orders\",\"nested\":{\"list\":[1,2],\"null\":null}}";
    emitter(1, 0).emit("spark", "SparkListenerJobEnd", payload, "3.5.0");
    assertEquals(1, bodies.size());
    assertTrue(bodies.get(0).contains(payload));
    assertTrue(bodies.get(0).contains("\"tool\":\"spark\""));
    assertTrue(bodies.get(0).contains("\"workspace\":\"acme\""));
  }

  @Test
  public void keyTravelsInTheHeaderNotTheBody() {
    emitter(1, 0).emit("spark", "e", "{}", null);
    assertEquals("Bearer secret-key", auth.get(0));
    assertFalse(bodies.get(0).contains("secret-key"));
  }

  @Test
  public void fullBatchIsOneRequest() {
    Emitter emitter = emitter(5, 0);
    for (int i = 0; i < 5; i++) {
      emitter.emit("spark", "e", "{\"i\":" + i + "}", null);
    }
    assertEquals(1, bodies.size());
    assertEquals(5, countOccurrences(bodies.get(0), "\"observation_id\""));
  }

  @Test
  public void closeFlushesAPartialBatch() {
    Emitter emitter = emitter(100, 0);
    emitter.emit("spark", "e", "{}", null);
    assertEquals(0, bodies.size());
    emitter.close();
    assertEquals(1, bodies.size());
  }

  @Test
  public void serverErrorNeverReachesTheCaller() {
    // A customer's job must not fail because our endpoint had a bad minute.
    status = 500;
    emitter(1, 0).emit("spark", "e", "{}", null);
  }

  @Test
  public void unreachableEndpointNeverReachesTheCaller() {
    Emitter emitter = new Emitter(Config.of("http://127.0.0.1:1", "k", "w", 1, 0));
    emitter.emit("spark", "e", "{}", null);
  }

  @Test
  public void rejectedRequestIsSentOnce() {
    // A 400 means the receiver understood us and said no.
    status = 400;
    emitter(1, 3).emit("spark", "e", "{}", null);
    assertEquals(1, bodies.size());
  }

  @Test
  public void transientFailureIsRetried() {
    status = 503;
    emitter(1, 2).emit("spark", "e", "{}", null);
    // The first attempt plus two retries.
    assertEquals(3, bodies.size());
  }

  @Test
  public void missingKeyIsReportedNotThrown() {
    assertNotNull(Config.of(url, null, "w", 1, 0).validate());
    new Emitter(Config.of(url, null, "w", 1, 0)).emit("spark", "e", "{}", null);
    assertEquals(0, bodies.size());
  }

  @Test
  public void usableConfigValidatesClean() {
    assertNull(Config.of(url, "k", "w", 1, 0).validate());
  }

  private static String read(InputStream in) throws java.io.IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toString("UTF-8");
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int at = haystack.indexOf(needle);
    while (at != -1) {
      count++;
      at = haystack.indexOf(needle, at + needle.length());
    }
    return count;
  }
}
