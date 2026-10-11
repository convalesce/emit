package io.convalesce.emit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The last lines a failed unit of work wrote, to send with its failure.
 *
 * <p>A job fails with a reason that is mostly stack. What went wrong is a few lines of it, and they
 * are sent as {@code output_tail}: credentials masked, then the end kept, at most {@link
 * #MAX_LINES} lines and {@link #MAX_BYTES} bytes of UTF-8. A job that succeeds sends none of it.
 *
 * <p>Masked, in this order, and declared once in the observation's {@code excluded} when anything
 * was: a PEM block, also one whose beginning is missing; a JSON Web Token; the value of a name that
 * reads as a credential, written {@code name=value}, {@code name: value} or {@code "name":
 * "value"}; and the password of a URI with user-info.
 *
 * <p>This is the Python client's {@code convalesce_emit.output} rule for rule, and {@code
 * testdata/output_vectors.json} holds the two to the same answer. On unless {@code
 * CONVALESCE_SEND_OUTPUT} is false. Nothing here throws, and a failure is logged by its class
 * alone: its message could quote the output.
 */
public final class Output {

  private static final Logger LOG = Logger.getLogger(Output.class.getName());

  /** The environment variable that switches this off. */
  public static final String SETTING = "CONVALESCE_SEND_OUTPUT";

  /** Where the tail goes in a payload, and what its {@code excluded} paths name. */
  public static final String FIELD = "output_tail";

  /** How many bytes of UTF-8 cross at most. */
  public static final int MAX_BYTES = 4096;

  /** How many lines cross at most. */
  public static final int MAX_LINES = 60;

  /** The reason declared for a tail with a credential masked in it. */
  public static final String MASKED = "credential masked";

  private static final String MASK = "***";
  private static final List<String> FALSY =
      Collections.unmodifiableList(Arrays.asList("0", "false", "no", "off"));

  private static final String PEM_BEGIN = "-----BEGIN";
  private static final String PEM_END = "-----END";
  // A line of a PEM block's body, as it reads once its beginning is gone, and the last of them,
  // which is as short as the block's length leaves it.
  private static final Pattern PEM_BODY = Pattern.compile("[A-Za-z0-9+/=]{16,}");
  private static final Pattern PEM_BODY_END = Pattern.compile("[A-Za-z0-9+/=]+");
  private static final Pattern JWT =
      Pattern.compile("eyJ[A-Za-z0-9_\\-]{4,}\\.[A-Za-z0-9_\\-]{4,}\\.[A-Za-z0-9_\\-]*");
  // `password=x`, `password: x`, `"password": "x"`, `Authorization: Bearer x`. The name may run
  // on (`secret_key`, `tokens`) and need not start a word (`db_password`); `sig` alone must be the
  // whole of one, or every `signal` would be. Spaces are spelled out, as in Python, so the two
  // mask exactly the same text.
  private static final Pattern ASSIGNED =
      Pattern.compile(
          "(?<head>(?:(?:password|passwd|pwd|passphrase|secret|token"
              + "|api[_\\-]?key|access[_\\-]?key|private[_\\-]?key|credential"
              + "|authorization|signature)[A-Za-z0-9_\\-]*|(?<![A-Za-z])sig)"
              + "[\"']?[ \\t]*[=:][ \\t]*"
              + "(?:(?:bearer|basic)[ \\t]+)?)"
              + "(?<secret>\"[^\"\\r\\n]*\"|'[^'\\r\\n]*'|[^ \\t\\r\\n&;,\"']+)",
          Pattern.CASE_INSENSITIVE);
  // The two rules `convalesce_emit.redact` masks any string by: the password of
  // `scheme://user:password@host`, and `password=...` in a query or connection string.
  private static final Pattern URI_PASSWORD =
      Pattern.compile("(?<head>[A-Za-z][A-Za-z0-9+.\\-]*://[^:@/\\s]*:)(?<secret>[^@/\\s]+)@");
  private static final Pattern PARAM_SECRET =
      Pattern.compile(
          "(?<head>(?:password|passwd|pwd|secret|token|api_?key|credential"
              + "|signature|(?<![A-Za-z])sig)=)(?<secret>[^&;\\s]+)",
          Pattern.CASE_INSENSITIVE);

  private Output() {}

  /**
   * Whether a failure's output may be sent.
   *
   * @param env the variables to read; the process's own when null
   * @return false only when {@code CONVALESCE_SEND_OUTPUT} says so
   */
  public static boolean enabled(Map<String, String> env) {
    String value = Config.setting(env == null ? System.getenv() : env, SETTING);
    return value == null || !FALSY.contains(value.trim().toLowerCase(Locale.ROOT));
  }

  /**
   * What is sent of a failed unit's output.
   *
   * @param output the output; null is none
   * @param env the variables to read; the process's own when null
   * @return the end of the output with its credentials masked, and what was masked or cut; {@link
   *     Tail#NOTHING} when there is none or output is not sent
   */
  public static Tail tail(String output, Map<String, String> env) {
    try {
      if (output == null || !enabled(env)) {
        return Tail.NOTHING;
      }
      return cut(output);
    } catch (Throwable t) {
      LOG.warning("convalesce: could not read the output: " + t.getClass().getSimpleName());
      return Tail.NOTHING;
    }
  }

  /**
   * Masks the credentials in a text, leaving the rest of it as it is.
   *
   * @param text output of any kind
   * @return the text with each credential found replaced by a mask
   */
  public static String mask(String text) {
    String out = maskPem(text);
    out = JWT.matcher(out).replaceAll(Matcher.quoteReplacement(MASK));
    out = ASSIGNED.matcher(out).replaceAll("${head}" + Matcher.quoteReplacement(MASK));
    out = URI_PASSWORD.matcher(out).replaceAll("${head}" + Matcher.quoteReplacement(MASK) + "@");
    return PARAM_SECRET.matcher(out).replaceAll("${head}" + Matcher.quoteReplacement(MASK));
  }

  private static Tail cut(String output) {
    // A character that cannot be encoded is a `?` in what is sent either way; replaced first, so
    // the byte count below is of what crosses.
    String text = new String(output.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
    List<String> lines = split(text.replace("\r\n", "\n").replace('\r', '\n'));
    while (!lines.isEmpty() && lines.get(lines.size() - 1).trim().isEmpty()) {
      lines.remove(lines.size() - 1);
    }
    List<Exclusion> excluded = new ArrayList<Exclusion>();
    String whole = join(lines);
    String masked = mask(whole);
    if (!masked.equals(whole)) {
      excluded.add(new Exclusion(FIELD, MASKED));
    }
    // Cut after the masking, so a credential is never cut in half and left unrecognisable.
    lines = split(masked);
    if (lines.size() > MAX_LINES) {
      lines = new ArrayList<String>(lines.subList(lines.size() - MAX_LINES, lines.size()));
      excluded.add(new Exclusion(FIELD, "limited to " + MAX_LINES + " lines"));
    }
    int size = lines.size() - 1;
    for (String line : lines) {
      size += line.getBytes(StandardCharsets.UTF_8).length;
    }
    int first = 0;
    while (size > MAX_BYTES && first < lines.size() - 1) {
      size -= lines.get(first).getBytes(StandardCharsets.UTF_8).length + 1;
      first++;
    }
    lines = new ArrayList<String>(lines.subList(first, lines.size()));
    boolean within = size <= MAX_BYTES;
    if (!within) {
      // One line longer than everything allowed: its end, from a whole character.
      byte[] data = lines.get(0).getBytes(StandardCharsets.UTF_8);
      int start = data.length - MAX_BYTES;
      while (start < data.length && (data[start] & 0xC0) == 0x80) {
        start++;
      }
      lines.set(0, new String(data, start, data.length - start, StandardCharsets.UTF_8));
    }
    if (first > 0 || !within) {
      excluded.add(new Exclusion(FIELD, "limited to " + MAX_BYTES + " bytes"));
    }
    return new Tail(join(lines), excluded);
  }

  private static String maskPem(String text) {
    if (!text.contains(PEM_BEGIN) && !text.contains(PEM_END)) {
      return text;
    }
    List<String> out = new ArrayList<String>();
    boolean inside = false;
    for (String line : split(text)) {
      if (inside) {
        inside = !line.contains(PEM_END);
        continue;
      }
      int begin = line.indexOf(PEM_BEGIN);
      if (begin >= 0) {
        // What is before the block on its line is kept; what is after it on its last line goes
        // with it.
        out.add(line.substring(0, begin) + MASK);
        inside = !line.substring(begin).contains(PEM_END);
      } else if (line.contains(PEM_END)) {
        // The end of a block whose beginning was already gone: the lines of its body are the
        // ones just above.
        Pattern body = PEM_BODY_END;
        while (!out.isEmpty() && body.matcher(strip(out.get(out.size() - 1))).matches()) {
          out.remove(out.size() - 1);
          body = PEM_BODY;
        }
        out.add(MASK);
      } else {
        out.add(line);
      }
    }
    return join(out);
  }

  // `String.split` drops trailing empty strings, and a line that is empty is still a line.
  private static List<String> split(String text) {
    List<String> out = new ArrayList<String>();
    int from = 0;
    while (true) {
      int at = text.indexOf('\n', from);
      if (at < 0) {
        out.add(text.substring(from));
        return out;
      }
      out.add(text.substring(from, at));
      from = at + 1;
    }
  }

  private static String join(List<String> lines) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < lines.size(); i++) {
      if (i > 0) {
        out.append('\n');
      }
      out.append(lines.get(i));
    }
    return out.toString();
  }

  // Python's `str.strip`, which also takes the characters `String.trim` leaves.
  private static String strip(String text) {
    int start = 0;
    int end = text.length();
    while (start < end && Character.isWhitespace(text.charAt(start))) {
      start++;
    }
    while (end > start && Character.isWhitespace(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(start, end);
  }

  /** What is sent of an output, and what was masked in it or cut from it. */
  public static final class Tail {

    static final Tail NOTHING = new Tail("", Collections.<Exclusion>emptyList());

    private final String text;
    private final List<Exclusion> excluded;

    Tail(String text, List<Exclusion> excluded) {
      this.text = text;
      this.excluded = excluded;
    }

    /** The tail; empty when there is nothing to send. */
    public String text() {
      return text;
    }

    /** The tail as a JSON string; null when there is nothing to send. */
    public String json() {
      return text.isEmpty() ? null : Json.quote(text);
    }

    /** What was masked or cut, by path and reason; empty when there is nothing to send. */
    public List<Exclusion> excluded() {
      return text.isEmpty() ? Collections.<Exclusion>emptyList() : excluded;
    }
  }
}
