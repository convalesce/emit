package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * A failure's output crosses as its last lines, with no credential among them.
 *
 * <p>{@code testdata/output_vectors.json} at the repository's root is what holds this and the
 * Python client to the same tail and the same declarations; its rows are read here as they are.
 */
public class OutputTest {

  private static final String VECTORS = "testdata/output_vectors.json";
  private static final Map<String, String> ON = Collections.<String, String>emptyMap();

  @Test
  public void everyTailInTheSharedVectorsIsTheOnePythonMakes() throws IOException {
    List<Object> rows = Vectors.list(Vectors.read(VECTORS).get("tails"));
    assertFalse(rows.isEmpty());
    for (Object each : rows) {
      Map<String, Object> row = Vectors.map(each);
      String name = (String) row.get("name");
      Output.Tail tail = Output.tail((String) row.get("output"), ON);
      assertEquals(name, row.get("tail"), tail.text());
      assertEquals(name, row.get("excluded"), reasons(tail));
      for (Exclusion exclusion : tail.excluded()) {
        assertEquals(name, "output_tail", exclusion.path());
      }
    }
  }

  @Test
  public void itIsOnUnlessTheSettingSaysNoUnderEitherName() {
    assertTrue(Output.enabled(ON));
    for (String no : new String[] {"false", "0", "no", "OFF", " False "}) {
      assertFalse(no, Output.enabled(Collections.singletonMap("CONVALESCE_SEND_OUTPUT", no)));
    }
    assertFalse(Output.enabled(Collections.singletonMap("CUSTOMER_CONVALESCE_SEND_OUTPUT", "no")));
    assertTrue(Output.enabled(Collections.singletonMap("CONVALESCE_SEND_OUTPUT", "true")));
  }

  @Test
  public void switchedOffNothingIsMadeOfAnOutput() {
    Map<String, String> off = new HashMap<String, String>();
    off.put("CONVALESCE_SEND_OUTPUT", "false");
    Output.Tail tail = Output.tail("it failed", off);
    assertEquals("", tail.text());
    assertNull(tail.json());
    assertTrue(tail.excluded().isEmpty());
  }

  @Test
  public void noOutputIsNothing() {
    assertNull(Output.tail(null, ON).json());
    assertNull(Output.tail("\n \n", ON).json());
  }

  @Test
  public void theEndIsKeptWithinBothLimits() {
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < 500; i++) {
      output.append(String.format("line %04d ", i));
      for (int j = 0; j < 150; j++) {
        output.append('x');
      }
      output.append('\n');
    }
    Output.Tail tail = Output.tail(output.toString(), ON);
    String[] lines = tail.text().split("\n");
    assertTrue(lines.length <= Output.MAX_LINES);
    assertTrue(tail.text().getBytes(StandardCharsets.UTF_8).length <= Output.MAX_BYTES);
    assertTrue(lines[lines.length - 1].startsWith("line 0499 "));
    List<String> expected = new ArrayList<String>();
    expected.add("limited to 60 lines");
    expected.add("limited to 4096 bytes");
    assertEquals(expected, reasons(tail));
  }

  @Test
  public void noSeededCredentialIsInWhatIsSent() {
    String output =
        "export DB_PASSWORD=hunter2\n"
            + "connecting to jdbc:postgresql://svc:hunter2@db.internal/app\n"
            + "-----BEGIN PRIVATE KEY-----\n"
            + "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
            + "-----END PRIVATE KEY-----\n"
            + "org.apache.spark.sql.AnalysisException: cannot resolve 'sales_amount'";
    Output.Tail tail = Output.tail(output, ON);
    assertFalse(tail.text().contains("hunter2"));
    assertFalse(tail.text().contains("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC"));
    assertFalse(tail.json().contains("hunter2"));
    assertTrue(tail.text().contains("svc:***@db.internal"));
    assertTrue(tail.text().contains("cannot resolve 'sales_amount'"));
    assertEquals(Collections.singletonList("credential masked"), reasons(tail));
  }

  @Test
  public void theTailIsOneJsonString() {
    assertEquals("\"a \\\"quoted\\\"\\nline\"", Output.tail("a \"quoted\"\nline", ON).json());
  }

  private static List<String> reasons(Output.Tail tail) {
    List<String> out = new ArrayList<String>();
    for (Exclusion exclusion : tail.excluded()) {
      out.add(exclusion.reason());
    }
    return out;
  }
}
