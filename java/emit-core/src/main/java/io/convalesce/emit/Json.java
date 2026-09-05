package io.convalesce.emit;

/**
 * The smallest JSON writer that will do.
 *
 * <p>There is no JSON library here on purpose. This jar goes into someone else's Spark driver, and
 * a dependency-free artifact cannot collide with whatever Jackson or json4s version that cluster
 * already runs, the same reason the Python client is built on {@code urllib}. Shading would solve
 * the collision too, at the cost of a fatter jar and a build step.
 *
 * <p>Only the envelope's own scalar fields are written here. The payload arrives as a JSON string
 * the tool itself produced and is embedded verbatim, so nothing in this class ever has to parse.
 */
final class Json {

  private Json() {}

  /**
   * Escapes a string and wraps it in quotes.
   *
   * @param value the string to write, which may be null
   * @return a JSON string literal, or the literal {@code null}
   */
  static String quote(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder out = new StringBuilder(value.length() + 2);
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        case '\b':
          out.append("\\b");
          break;
        case '\f':
          out.append("\\f");
          break;
        default:
          // Control characters have no literal form and must be escaped; anything else, including
          // non-ASCII, is valid UTF-8 JSON as written.
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    out.append('"');
    return out.toString();
  }
}
