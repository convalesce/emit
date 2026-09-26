package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Drives the check against a real HTTP server: what it sends, and what it makes of each answer, is
 * the claim being tested.
 */
public class CheckTest {

  private static final String KEY = "cvl_ingest_abc123_s3cr3t-value";

  private HttpServer server;
  private String endpoint;
  private final List<String> paths = Collections.synchronizedList(new ArrayList<String>());
  private final List<String> auth = Collections.synchronizedList(new ArrayList<String>());
  private volatile int status = 202;

  @Before
  public void startServer() throws Exception {
    paths.clear();
    auth.clear();
    status = 202;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws java.io.IOException {
            InputStream body = exchange.getRequestBody();
            while (body.read() != -1) {
              // Drain the request before answering.
            }
            paths.add(exchange.getRequestURI().getPath());
            auth.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
          }
        });
    server.start();
    endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/openapi";
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  private static String run(Config config, int expected) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bytes, true, "UTF-8");
    assertEquals(expected, Check.run(config, out));
    return bytes.toString("UTF-8");
  }

  @Test
  public void anAcceptedKeyIsConnectedWithOneEmptyBatch() throws Exception {
    String text = run(Config.of(endpoint, KEY, 50, 0), Check.CONNECTED);

    assertTrue(text, text.contains("Connected"));
    assertEquals(Collections.singletonList("/openapi/v1/observations"), paths);
    assertEquals("Bearer " + KEY, auth.get(0));
  }

  @Test
  public void eachRefusalSaysWhatToFixAndNeverPrintsTheSecret() throws Exception {
    int[] statuses = {401, 403, 404, 500};
    String[] expected = {"refused", "ingest key", "/openapi", "HTTP 500"};
    for (int i = 0; i < statuses.length; i++) {
      status = statuses[i];
      String text = run(Config.of(endpoint, KEY, 50, 0), Check.NOT_CONNECTED);
      assertTrue(text, text.contains(expected[i]));
      assertTrue(text, text.contains("cvl_ingest_abc123_"));
      assertFalse(text, text.contains("s3cr3t"));
    }
  }

  @Test
  public void anEndpointNothingListensOnIsReportedAsUnreachable() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();

    String text = run(Config.of("http://127.0.0.1:" + port, KEY, 50, 0), Check.NOT_CONNECTED);

    assertTrue(text, text.contains("could not reach"));
  }

  @Test
  public void aConfigurationThatWouldSendNothingIsNotChecked() throws Exception {
    String text = run(Config.of(endpoint, null, 50, 0), Check.MISCONFIGURED);

    assertTrue(text, text.contains("CONVALESCE_INGEST_KEY"));
    assertTrue(paths.isEmpty());
  }

  @Test
  public void onlyAKeysPublicPartIsShown() {
    assertEquals("cvl_ingest_abc123_...", Check.keyId(KEY));
    assertEquals("(not a Convalesce key)", Check.keyId("not-a-key"));
    assertEquals("(not a Convalesce key)", Check.keyId(null));
  }
}
