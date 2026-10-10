package io.convalesce.emit;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The settings a run had, with every secret among them sent as a keyed hash.
 *
 * <p>A run's outcome can turn on a value nothing in its event carries: an environment variable on
 * its driver. A receiver that holds them for the last good run and for this one can say which
 * changed. That is all this is for, and it is off until {@code CONVALESCE_SEND_SETTINGS} turns it
 * on.
 *
 * <p>A setting crosses in one of two forms. An ordinary one crosses as its value. One whose name or
 * value reads as a credential, or that is too long to be sure of, crosses as an HMAC-SHA-256 of its
 * kind, name and value, cut to sixteen hex characters: enough to tell that it changed, and nothing
 * of what it is. A bare hash of a short value could be reversed by trying candidates, so it is
 * keyed. The key is {@code CONVALESCE_FINGERPRINT_KEY} where that is set, which never leaves the
 * process, and otherwise one derived from the ingest key under a fixed context. With neither, no
 * hash is made and the setting is declared as left out.
 *
 * <p>Hashes made under different keys cannot be compared, so they travel with {@code keyed_by}: a
 * hash of the key itself, equal between two events only when their key was.
 *
 * <p>Neither a key nor a hashed value is logged or sent.
 *
 * <p>This is the Python client's {@code convalesce_emit.settings}, rule for rule, and {@code
 * testdata/settings_vectors.json} holds both to the same hashes and the same decisions. Text is
 * read by code point wherever Python reads it by character (a value's length, the order of names,
 * the bytes hashed), so the two agree on any string they are both handed. What they cannot agree on
 * is a variable whose bytes are not valid text: Python keeps each such byte as a lone surrogate and
 * the JVM has already replaced it by the time {@code System.getenv()} returns, so the two hash
 * different strings.
 */
public final class Settings {

  private static final Logger LOG = Logger.getLogger(Settings.class.getName());

  /** Switches the whole thing on. */
  public static final String SETTING = "CONVALESCE_SEND_SETTINGS";

  /** A key of the customer's own to hash with, which is never sent. */
  public static final String KEY_SETTING = "CONVALESCE_FINGERPRINT_KEY";

  /** Names never sent in either form, comma-separated. */
  public static final String SKIP_SETTING = "CONVALESCE_SETTINGS_SKIP";

  /** Where the settings go in a payload, and what {@code excluded} paths start with. */
  public static final String FIELD = "settings";

  public static final String ITEMS = "items";
  public static final String KEYED_BY = "keyed_by";
  public static final String ENVIRONMENT = "environment";
  public static final String VARIABLE = "variable";

  /**
   * How many settings of one kind cross. A driver's environment is a few hundred names at most;
   * past this something else is being listed.
   */
  public static final int MAX_ITEMS = 500;

  /**
   * A value longer than this is hashed whatever it holds: a certificate, a JSON document of
   * connection details, a script.
   */
  public static final int MAX_VALUE_CHARS = 300;

  // What a key is mixed with to make the key hashes are made under. Changing it changes every
  // hash, and `keyed_by` with them.
  private static final String CONTEXT = "convalesce-emit/fingerprint/v1";
  // Sixteen hex characters, 64 bits: two different values of one setting meet by chance about once
  // in 2**64, and less of the digest crosses.
  private static final int LENGTH = 16;
  private static final String HMAC = "HmacSHA256";
  private static final Set<String> TRUTHY = setOf("1", "true", "yes", "on");

  // Matched against a name lower-cased with everything but letters and digits removed. Substrings,
  // and wide on purpose: a name caught by mistake is hashed, which costs a value and never leaks
  // one.
  private static final List<String> SECRET_NAME_PARTS =
      Collections.unmodifiableList(
          Arrays.asList(
              "key",
              "token",
              "secret",
              "auth",
              "pass",
              "pwd",
              "credential",
              "cookie",
              "sas",
              "signature",
              "private",
              "cert",
              "dsn",
              // A connection is where a password usually lives.
              "conn"));

  // What Python's `\s` matches in text, which is more than Java's: written out so that a value
  // with a no-break space before its `=` is hashed here as it is there.
  private static final String SPACE =
      "\\t\\n\\x0B\\f\\r\\x1c-\\x1f \\x85\\xa0\\u1680"
          + "\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000";
  private static final Pattern URI_USERINFO =
      Pattern.compile("[A-Za-z][A-Za-z0-9+.\\-]*://[^/@" + SPACE + "]*:[^/@" + SPACE + "]+@");
  // UNICODE_CASE because Python ignores case that way: a long s reads as an s in both.
  private static final Pattern ASSIGNED =
      Pattern.compile(
          "(password|passwd|pwd|secret|token|api[_-]?key|credential|signature|sig)[\"']?["
              + SPACE
              + "]*[=:]",
          Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
  private static final String PEM = "-----BEGIN";
  private static final Pattern JWT = Pattern.compile("eyJ[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+\\.");
  private static final Pattern TOKEN_CHARS = Pattern.compile("[A-Za-z0-9+/=_\\-.~:]+");
  // How long an unbroken run of token characters is before its randomness is looked at, and how
  // random it has to be, in bits a character. English and file paths sit near 3; a generated key
  // near 4.5 and above.
  private static final int TOKEN_MIN_CHARS = 20;
  private static final double TOKEN_MIN_ENTROPY = 3.5;
  // This library's own keys. They are what a hash is keyed with, so a hash of one would be made
  // with itself.
  private static final Set<String> OWN_KEYS =
      setOf("CONVALESCE_INGEST_KEY", "CONVALESCE_API_KEY", KEY_SETTING);

  // Python orders text by code point and Java by UTF-16 unit, which differ once a name holds a
  // character outside the basic plane.
  private static final Comparator<String> BY_CODE_POINT =
      new Comparator<String>() {
        @Override
        public int compare(String left, String right) {
          int i = 0;
          int j = 0;
          while (i < left.length() && j < right.length()) {
            int a = left.codePointAt(i);
            int b = right.codePointAt(j);
            if (a != b) {
              return a < b ? -1 : 1;
            }
            i += Character.charCount(a);
            j += Character.charCount(b);
          }
          return (left.length() - i) - (right.length() - j);
        }
      };

  private Settings() {}

  /**
   * Whether settings are sent at all. They are not unless asked for.
   *
   * @param env the variables to read; the process's own when null
   * @return whether {@code CONVALESCE_SEND_SETTINGS} is switched on
   */
  public static boolean enabled(Map<String, String> env) {
    String value = read(env, SETTING);
    return value != null && TRUTHY.contains(strip(value).toLowerCase(Locale.ROOT));
  }

  /**
   * The key hashes are made under.
   *
   * @param secret the fingerprint key, or the ingest key where there is none
   * @return a key that says nothing of {@code secret} to anyone without it
   */
  public static byte[] derive(String secret) {
    return hmac(utf8(secret, false), utf8(CONTEXT, false));
  }

  /**
   * The key this process hashes with.
   *
   * @param ingestKey the key this process sends with, if it has one
   * @param env the variables to read; the process's own when null
   * @return the key, or null when there is nothing to make one from
   */
  public static byte[] keyOf(String ingestKey, Map<String, String> env) {
    String own = read(env, KEY_SETTING);
    own = own == null ? "" : strip(own);
    String secret = !own.isEmpty() ? own : (ingestKey == null ? "" : ingestKey);
    return secret.isEmpty() ? null : derive(secret);
  }

  /**
   * Hashes one value.
   *
   * @param key what {@link #keyOf} returned
   * @param kind {@code environment}, {@code variable} or another kind a plugin names
   * @param name the setting's name
   * @param value its value; hashed, and not kept
   * @return the same sixteen hex characters for the same three, and others for any other
   */
  public static String of(byte[] key, String kind, String name, String value) {
    // A NUL cannot be in an environment variable's name or value, so no two different triples read
    // as the same bytes.
    byte[] digest = hmac(key, utf8(kind + "\0" + name + "\0" + value, true));
    StringBuilder out = new StringBuilder(LENGTH);
    for (int i = 0; i < LENGTH / 2; i++) {
      out.append(Character.forDigit((digest[i] >> 4) & 0xf, 16));
      out.append(Character.forDigit(digest[i] & 0xf, 16));
    }
    return out.toString();
  }

  /**
   * Whether a setting crosses as a hash rather than as its value.
   *
   * @param name the setting's name
   * @param value its value
   * @return whether its name or its value reads as a credential, or it is too long to be sure of
   */
  public static boolean isSecret(String name, String value) {
    if (value.codePointCount(0, value.length()) > MAX_VALUE_CHARS) {
      return true;
    }
    String flat = flat(name);
    for (String part : SECRET_NAME_PARTS) {
      if (flat.contains(part)) {
        return true;
      }
    }
    if (value.contains(PEM)
        || URI_USERINFO.matcher(value).find()
        || ASSIGNED.matcher(value).find()) {
      return true;
    }
    if (JWT.matcher(value).lookingAt()) {
      return true;
    }
    return readsAsToken(value);
  }

  /**
   * Turns a run's settings into what is sent for them.
   *
   * <p>Never throws: a run must not fail because its settings could not be read.
   *
   * @param ingestKey the key this process sends with, if it has one
   * @param kinds the settings, by kind and then by name
   * @param env where this library's own settings are read; the process's variables when null
   * @return what to send under {@code settings}, empty when there is nothing to send, and
   *     everything that was left out, by path and reason
   */
  public static Collected collect(
      String ingestKey, Map<String, ? extends Map<String, String>> kinds, Map<String, String> env) {
    try {
      if (!enabled(env)) {
        return Collected.NOTHING;
      }
      byte[] key = keyOf(ingestKey, env);
      Set<String> skip = skipped(env);
      List<Exclusion> excluded = new ArrayList<Exclusion>();
      StringBuilder items = new StringBuilder();
      boolean hashed = false;
      int unkeyed = 0;
      List<String> ordered = new ArrayList<String>(kinds.keySet());
      Collections.sort(ordered, BY_CODE_POINT);
      for (String kind : ordered) {
        Map<String, String> held = kinds.get(kind);
        List<String> names = new ArrayList<String>();
        for (String name : held.keySet()) {
          // A name with no value is a setting that was not set.
          if (held.get(name) != null && wanted(name, skip)) {
            names.add(name);
          }
        }
        Collections.sort(names, BY_CODE_POINT);
        if (names.size() > MAX_ITEMS) {
          names = names.subList(0, MAX_ITEMS);
          excluded.add(new Exclusion(FIELD + "." + kind, "limited to " + MAX_ITEMS + " names"));
        }
        for (String name : names) {
          String value = held.get(name);
          String form;
          if (!isSecret(name, value)) {
            form = "\"value\":" + Json.quote(value);
          } else if (key == null) {
            unkeyed++;
            continue;
          } else {
            form = "\"fingerprint\":" + Json.quote(of(key, kind, name, value));
            hashed = true;
          }
          items.append(items.length() == 0 ? "" : ",");
          items.append("{\"kind\":").append(Json.quote(kind));
          items.append(",\"name\":").append(Json.quote(name));
          items.append(',').append(form).append('}');
        }
      }
      if (unkeyed > 0) {
        excluded.add(new Exclusion(FIELD, "no key to fingerprint " + unkeyed + " settings with"));
      }
      if (items.length() == 0) {
        return new Collected(null, excluded);
      }
      StringBuilder out = new StringBuilder(items.length() + 64);
      out.append("{\"").append(ITEMS).append("\":[").append(items).append(']');
      if (hashed) {
        out.append(",\"")
            .append(KEYED_BY)
            .append("\":")
            .append(Json.quote(of(key, KEYED_BY, "", "")));
      }
      return new Collected(out.append('}').toString(), excluded);
    } catch (Throwable t) {
      // The exception's class and not its message, which may hold a value.
      LOG.warning("convalesce: could not collect settings: " + t.getClass().getSimpleName());
      return Collected.NOTHING;
    }
  }

  /** What {@link #collect} found: the field to send, and what it left out of it. */
  public static final class Collected {

    static final Collected NOTHING = new Collected(null, Collections.<Exclusion>emptyList());

    private final String json;
    private final List<Exclusion> excluded;

    Collected(String json, List<Exclusion> excluded) {
      this.json = json;
      this.excluded = Collections.unmodifiableList(excluded);
    }

    /**
     * The value of a payload's {@code settings} field.
     *
     * @return {@code {"items":[{"kind","name","value"|"fingerprint"},...],"keyed_by":"..."}}, with
     *     {@code keyed_by} only where an item is hashed, or null when there is nothing to send
     */
    public String json() {
      return json;
    }

    /** Whether there is nothing to send. */
    public boolean isEmpty() {
      return json == null;
    }

    /** What was left out, for the observation's {@code excluded}. */
    public List<Exclusion> excluded() {
      return excluded;
    }
  }

  private static String read(Map<String, String> env, String name) {
    return Config.setting(env == null ? System.getenv() : env, name);
  }

  /** The names {@code CONVALESCE_SETTINGS_SKIP} says are never sent, exactly as written. */
  private static Set<String> skipped(Map<String, String> env) {
    Set<String> out = new HashSet<String>();
    String listed = read(env, SKIP_SETTING);
    if (listed != null) {
      for (String each : listed.split(",")) {
        String name = strip(each);
        if (!name.isEmpty()) {
          out.add(name);
        }
      }
    }
    return out;
  }

  /** False for a name the operator listed and for this library's own keys. */
  private static boolean wanted(String name, Set<String> skip) {
    if (name == null || skip.contains(name)) {
      return false;
    }
    if (name.startsWith(Config.PLATFORM_PREFIX)
        && OWN_KEYS.contains(name.substring(Config.PLATFORM_PREFIX.length()))) {
      return false;
    }
    return !OWN_KEYS.contains(name);
  }

  /** A name lower-cased, with everything but letters and digits removed. */
  private static String flat(String name) {
    String lower = name.toLowerCase(Locale.ROOT);
    StringBuilder out = new StringBuilder(lower.length());
    for (int i = 0; i < lower.length(); ) {
      int c = lower.codePointAt(i);
      i += Character.charCount(c);
      // Python's `isalnum`: a letter, or a number of any of the three kinds.
      int type = Character.getType(c);
      if (Character.isLetter(c)
          || type == Character.DECIMAL_DIGIT_NUMBER
          || type == Character.LETTER_NUMBER
          || type == Character.OTHER_NUMBER) {
        out.appendCodePoint(c);
      }
    }
    return out.toString();
  }

  /**
   * Whether a value looks generated rather than written: one long run of token characters, with
   * letters and digits both, as random as a generated key is.
   */
  private static boolean readsAsToken(String value) {
    // Python's `$` also matches before one closing newline, so a value read from a file with its
    // line end still counts; the newline is then one more character in the sums below.
    String run = value.endsWith("\n") ? value.substring(0, value.length() - 1) : value;
    if (value.length() < TOKEN_MIN_CHARS || !TOKEN_CHARS.matcher(run).matches()) {
      return false;
    }
    // Past this point every character is ASCII, so counting UTF-16 units is counting what Python
    // counts. A character outside the basic plane, two units here and one there, never gets this
    // far.
    boolean digit = false;
    boolean letter = false;
    Map<Character, Integer> counts = new LinkedHashMap<Character, Integer>();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      digit |= c >= '0' && c <= '9';
      letter |= (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
      Integer seen = counts.get(c);
      counts.put(c, seen == null ? 1 : seen + 1);
    }
    if (!digit || !letter) {
      return false;
    }
    double total = value.length();
    double sum = 0.0;
    for (int n : counts.values()) {
      double share = n / total;
      sum += share * log2(share);
    }
    return -sum >= TOKEN_MIN_ENTROPY;
  }

  /**
   * A base-two logarithm, exact for a power of two as Python's is.
   *
   * <p>That keeps a value whose randomness is exactly the threshold on the same side in both. For
   * any other share the two may differ in the last bit, which decides nothing unless the sum lands
   * within that bit of 3.5.
   */
  private static double log2(double x) {
    int exponent = Math.getExponent(x);
    if (x == Math.scalb(1.0, exponent)) {
      return exponent;
    }
    return Math.log(x) / Math.log(2.0);
  }

  /** Python's {@code strip}: Unicode white space off both ends, where {@code trim} knows ASCII. */
  private static String strip(String text) {
    int start = 0;
    int end = text.length();
    while (start < end && space(text.charAt(start))) {
      start++;
    }
    while (end > start && space(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(start, end);
  }

  private static boolean space(char c) {
    return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085';
  }

  private static byte[] hmac(byte[] key, byte[] message) {
    try {
      Mac mac = Mac.getInstance(HMAC);
      // HMAC pads a key with zeros, so one zero byte is the empty key the JDK refuses to take.
      mac.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, HMAC));
      return mac.doFinal(message);
    } catch (java.security.GeneralSecurityException e) {
      // Every JVM is required to have HmacSHA256.
      throw new IllegalStateException(e.getClass().getSimpleName());
    }
  }

  /**
   * Text as UTF-8.
   *
   * <p>{@code String.getBytes} writes a question mark for half of a surrogate pair on its own, so
   * two values that differ only there would hash alike. Python writes it as three bytes ({@code
   * surrogatepass}) where a value is hashed and refuses it in a key, and so does this.
   *
   * @param lone whether half a pair on its own is written rather than refused
   */
  private static byte[] utf8(String text, boolean lone) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(text.length() + 16);
    for (int i = 0; i < text.length(); ) {
      int c = text.codePointAt(i);
      i += Character.charCount(c);
      if (c < 0x80) {
        out.write(c);
      } else if (c < 0x800) {
        out.write(0xc0 | (c >> 6));
        out.write(0x80 | (c & 0x3f));
      } else if (c < 0x10000) {
        if (!lone && c >= Character.MIN_SURROGATE && c <= Character.MAX_SURROGATE) {
          throw new IllegalArgumentException("half a surrogate pair in a key");
        }
        out.write(0xe0 | (c >> 12));
        out.write(0x80 | ((c >> 6) & 0x3f));
        out.write(0x80 | (c & 0x3f));
      } else {
        out.write(0xf0 | (c >> 18));
        out.write(0x80 | ((c >> 12) & 0x3f));
        out.write(0x80 | ((c >> 6) & 0x3f));
        out.write(0x80 | (c & 0x3f));
      }
    }
    return out.toByteArray();
  }

  private static Set<String> setOf(String... values) {
    return Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(values)));
  }
}
