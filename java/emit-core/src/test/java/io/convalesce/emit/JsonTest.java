package io.convalesce.emit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Tests the envelope's own escaping; the payload is never re-encoded. */
public class JsonTest {

  @Test
  public void escapesWhatJsonRequires() {
    assertEquals("\"a\\\"b\"", Json.quote("a\"b"));
    assertEquals("\"a\\\\b\"", Json.quote("a\\b"));
    assertEquals("\"a\\nb\"", Json.quote("a\nb"));
  }

  @Test
  public void escapesControlCharacters() {
    // A control character has no literal form in JSON, so a raw one would produce a body the
    // receiver could not parse.
    assertEquals("\"a\\u0000b\"", Json.quote("a\u0000b"));
    assertEquals("\"\\u001f\"", Json.quote("\u001f"));
  }

  @Test
  public void leavesValidUtf8Alone() {
    // Non-ASCII needs no escape in UTF-8 JSON, and escaping it would only bloat the payload.
    assertEquals("\"caf\u00e9\"", Json.quote("caf\u00e9"));
  }

  @Test
  public void nullIsTheJsonLiteral() {
    assertEquals("null", Json.quote(null));
  }
}
