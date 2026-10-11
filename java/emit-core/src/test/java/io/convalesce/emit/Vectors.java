package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The files under {@code testdata/} at the repository's root, which hold this library and the
 * Python client to the same answers. Each is read here as it is.
 */
final class Vectors {

  private Vectors() {}

  /**
   * Reads one file of vectors, found by walking up from wherever Gradle ran the tests.
   *
   * @param path the file, from the repository's root
   * @return the object it holds
   */
  static Map<String, Object> read(String path) throws IOException {
    File at = new File(System.getProperty("user.dir")).getAbsoluteFile();
    while (at != null && !new File(at, path).isFile()) {
      at = at.getParentFile();
    }
    assertTrue(path + " not found above " + System.getProperty("user.dir"), at != null);
    byte[] bytes = Files.readAllBytes(new File(at, path).toPath());
    return parse(new String(bytes, StandardCharsets.UTF_8));
  }

  /**
   * Reads one JSON object.
   *
   * @param json the object as text
   * @return the object
   */
  static Map<String, Object> parse(String json) {
    return map(new Reader(json).value());
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> map(Object value) {
    return (Map<String, Object>) value;
  }

  @SuppressWarnings("unchecked")
  static List<Object> list(Object value) {
    return (List<Object>) value;
  }

  /** Just enough of a JSON reader for the vectors: there is no JSON library here to borrow. */
  private static final class Reader {

    private final String text;
    private int at;

    Reader(String text) {
      this.text = text;
    }

    Object value() {
      space();
      char c = text.charAt(at);
      if (c == '{') {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        at++;
        for (space(); text.charAt(at) != '}'; space()) {
          String key = string();
          space();
          expect(':');
          out.put(key, value());
          space();
          if (text.charAt(at) == ',') {
            at++;
          }
        }
        at++;
        return out;
      }
      if (c == '[') {
        List<Object> out = new ArrayList<Object>();
        at++;
        for (space(); text.charAt(at) != ']'; space()) {
          out.add(value());
          space();
          if (text.charAt(at) == ',') {
            at++;
          }
        }
        at++;
        return out;
      }
      if (c == '"') {
        return string();
      }
      int start = at;
      while (at < text.length() && ",}] \n\r\t".indexOf(text.charAt(at)) < 0) {
        at++;
      }
      String word = text.substring(start, at);
      if (word.equals("true") || word.equals("false")) {
        return Boolean.valueOf(word);
      }
      return word.equals("null") ? null : (Object) Double.valueOf(word);
    }

    private String string() {
      expect('"');
      StringBuilder out = new StringBuilder();
      while (text.charAt(at) != '"') {
        char c = text.charAt(at++);
        if (c != '\\') {
          out.append(c);
          continue;
        }
        char escaped = text.charAt(at++);
        switch (escaped) {
          case 'n':
            out.append('\n');
            break;
          case 't':
            out.append('\t');
            break;
          case 'r':
            out.append('\r');
            break;
          case 'b':
            out.append('\b');
            break;
          case 'f':
            out.append('\f');
            break;
          case 'u':
            out.append((char) Integer.parseInt(text.substring(at, at + 4), 16));
            at += 4;
            break;
          default:
            out.append(escaped);
        }
      }
      at++;
      return out.toString();
    }

    private void expect(char c) {
      assertEquals("at " + at, c, text.charAt(at));
      at++;
    }

    private void space() {
      while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
        at++;
      }
    }
  }
}
