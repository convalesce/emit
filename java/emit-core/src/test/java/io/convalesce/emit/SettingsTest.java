package io.convalesce.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * A run's settings cross as their values, and the secrets among them only as keyed hashes.
 *
 * <p>{@code testdata/settings_vectors.json} at the repository's root is what holds this and the
 * Python client to the same hashes and the same decisions; its rows are read here as they are.
 */
public class SettingsTest {

  private static final String VECTORS = "testdata/settings_vectors.json";

  @Test
  public void everyHashInTheSharedVectorsIsTheOnePythonMakes() throws IOException {
    List<Object> rows = list(vectors().get("hashes"));
    assertFalse(rows.isEmpty());
    for (Object each : rows) {
      Map<String, Object> row = map(each);
      String made =
          Settings.of(
              Settings.derive((String) row.get("secret")),
              (String) row.get("kind"),
              (String) row.get("name"),
              (String) row.get("value"));
      assertEquals(String.valueOf(row), row.get("fingerprint"), made);
    }
  }

  @Test
  public void everySettingInTheSharedVectorsIsPlainOrSecretAsInPython() throws IOException {
    List<Object> rows = list(vectors().get("secret"));
    assertFalse(rows.isEmpty());
    for (Object each : rows) {
      Map<String, Object> row = map(each);
      String name = (String) row.get("name");
      String value = (String) row.get("value");
      assertEquals(name + "=" + value, row.get("is_secret"), Settings.isSecret(name, value));
    }
  }

  @Test
  public void halfASurrogatePairIsHashedAsPythonHashesIt() {
    // Made with the Python client: `of(derive("ingest"), "environment", ...)`.
    byte[] key = Settings.derive("ingest");
    assertEquals("024cb7c49cd6ae89", Settings.of(key, "environment", "HALF", "a\ud800b"));
    assertEquals("6a5057434ceb3236", Settings.of(key, "environment", "ASTRAL", "😀"));
    // And not as the question mark `String.getBytes` would have written for it.
    assertEquals("174e8535773cbd6a", Settings.of(key, "environment", "HALF", "a?b"));
  }

  @Test
  public void aValueReadWithItsLineEndIsStillAToken() {
    assertTrue(Settings.isSecret("SOMETHING", "Zq9aF3kQ7zL2mX8vB4nC6tR1yU0p\n"));
    assertFalse(Settings.isSecret("SOMETHING", "a-plain-sentence-of-words-only\n"));
  }

  @Test
  public void aSpacePythonCountsBeforeAnEqualsSignCountsHere() {
    assertTrue(Settings.isSecret("SOMETHING", "Server=db;Password =hunter2"));
    assertTrue(Settings.isSecret("SOMETHING", "ſecret=hunter2"));
  }

  @Test
  public void nothingIsSentUnlessAskedFor() {
    assertFalse(Settings.enabled(env()));
    assertFalse(Settings.enabled(env("CONVALESCE_SEND_SETTINGS", "0")));
    assertFalse(Settings.enabled(env("CONVALESCE_SEND_SETTINGS", "false")));
    for (String yes : new String[] {"1", "true", "yes", "on", " TRUE ", "On"}) {
      assertTrue(yes, Settings.enabled(env("CONVALESCE_SEND_SETTINGS", yes)));
    }
    assertTrue(Settings.enabled(env("CUSTOMER_CONVALESCE_SEND_SETTINGS", "on")));

    Settings.Collected off = Settings.collect("ingest", kinds(env("A", "1")), env());
    assertTrue(off.isEmpty());
    assertNull(off.json());
    assertTrue(off.excluded().isEmpty());
  }

  @Test
  public void anOrdinarySettingCrossesAsItsValueAndASecretAsAHash() {
    Settings.Collected found =
        Settings.collect(
            "ingest", kinds(env("TZ", "Europe/London", "DB_PASSWORD", "hunter2")), on());
    assertEquals(
        "{\"items\":["
            + "{\"kind\":\"environment\",\"name\":\"DB_PASSWORD\",\"fingerprint\":\"7ee700b0447bb731\"},"
            + "{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"Europe/London\"}],"
            + "\"keyed_by\":\"2b4957b70d863694\"}",
        found.json());
    assertTrue(found.excluded().isEmpty());
    assertFalse(found.json().contains("hunter2"));
  }

  @Test
  public void withNothingHashedNoKeyIsNamed() {
    Settings.Collected found = Settings.collect("ingest", kinds(env("TZ", "UTC")), on());
    assertEquals(
        "{\"items\":[{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"UTC\"}]}", found.json());
  }

  @Test
  public void theCustomersOwnKeyIsUsedBeforeTheIngestKey() {
    Map<String, String> own = on("CONVALESCE_FINGERPRINT_KEY", " another-key ");
    Settings.Collected found =
        Settings.collect("ingest", kinds(env("DB_PASSWORD", "hunter2")), own);
    // The vectors' row for `another-key`.
    assertTrue(found.json(), found.json().contains("\"fingerprint\":\"160070ebd0e2c589\""));
    String keyedBy = Settings.of(Settings.derive("another-key"), "keyed_by", "", "");
    assertTrue(found.json().contains("\"keyed_by\":\"" + keyedBy + "\""));
    assertFalse(found.json().contains("another-key"));

    Map<String, String> prefixed = on("CUSTOMER_CONVALESCE_FINGERPRINT_KEY", "another-key");
    assertEquals(
        found.json(),
        Settings.collect("ingest", kinds(env("DB_PASSWORD", "hunter2")), prefixed).json());
    assertEquals(
        found.json(), Settings.collect(null, kinds(env("DB_PASSWORD", "hunter2")), own).json());
  }

  @Test
  public void withNoKeyASecretIsLeftOutAndThatIsDeclared() {
    Map<String, String> held = env("TZ", "UTC", "DB_PASSWORD", "hunter2", "API_TOKEN", "abc");
    for (String none : new String[] {null, ""}) {
      Settings.Collected found = Settings.collect(none, kinds(held), on());
      assertEquals(
          "{\"items\":[{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"UTC\"}]}",
          found.json());
      assertEquals(1, found.excluded().size());
      assertEquals("settings", found.excluded().get(0).path());
      assertEquals("no key to fingerprint 2 settings with", found.excluded().get(0).reason());
    }
    // With nothing but secrets there is no field at all, and the declaration still stands.
    Settings.Collected only = Settings.collect(null, kinds(env("DB_PASSWORD", "hunter2")), on());
    assertTrue(only.isEmpty());
    assertEquals("no key to fingerprint 1 settings with", only.excluded().get(0).reason());
  }

  @Test
  public void aSkippedNameAndThisLibrarysOwnKeysAreNeverSent() {
    Map<String, String> held =
        env(
            "CONVALESCE_INGEST_KEY", "cvl_ingest_abc_secret",
            "CONVALESCE_API_KEY", "cvl_api_abc_secret",
            "CONVALESCE_FINGERPRINT_KEY", "another-key",
            "CUSTOMER_CONVALESCE_INGEST_KEY", "cvl_ingest_abc_secret",
            "CUSTOMER_CONVALESCE_API_KEY", "cvl_api_abc_secret",
            "CUSTOMER_CONVALESCE_FINGERPRINT_KEY", "another-key",
            "INTERNAL_HOST", "db.internal",
            "DB_PASSWORD", "hunter2",
            "TZ", "UTC");
    Map<String, String> settings = on("CONVALESCE_SETTINGS_SKIP", " INTERNAL_HOST , DB_PASSWORD,,");
    Settings.Collected found = Settings.collect("ingest", kinds(held), settings);
    assertEquals(
        "{\"items\":[{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"UTC\"}]}", found.json());
    assertTrue(found.excluded().isEmpty());

    // The list is read under the platform's prefix too, and names are matched exactly.
    Map<String, String> prefixed = on("CUSTOMER_CONVALESCE_SETTINGS_SKIP", "tz");
    String sent = Settings.collect("ingest", kinds(held), prefixed).json();
    assertTrue(sent, sent.contains("\"name\":\"TZ\""));
    assertTrue(sent, sent.contains("\"name\":\"INTERNAL_HOST\",\"value\":\"db.internal\""));
    assertFalse(sent, sent.contains("CONVALESCE_"));
    assertFalse(sent, sent.contains("secret"));
  }

  @Test
  public void aKindIsLimitedToFiveHundredNamesAndSaysSo() {
    Map<String, String> many = new HashMap<String, String>();
    for (int i = 0; i < Settings.MAX_ITEMS + 3; i++) {
      many.put(String.format("N%04d", i), "v");
    }
    Map<String, Map<String, String>> kinds = new HashMap<String, Map<String, String>>();
    kinds.put("environment", many);
    kinds.put("variable", env("a", "1"));
    Settings.Collected found = Settings.collect("ingest", kinds, on());
    assertEquals(Settings.MAX_ITEMS + 1, count(found.json(), "{\"kind\":"));
    assertTrue(found.json().contains("\"name\":\"N0499\""));
    assertFalse(found.json().contains("\"name\":\"N0500\""));
    assertEquals(1, found.excluded().size());
    assertEquals("settings.environment", found.excluded().get(0).path());
    assertEquals("limited to 500 names", found.excluded().get(0).reason());
    assertEquals(
        "{\"path\":\"settings.environment\",\"reason\":\"limited to 500 names\"}",
        found.excluded().get(0).toJson());
  }

  @Test
  public void theOrderIsByKindAndThenByNameWhateverOrderTheyCameIn() {
    Map<String, Map<String, String>> kinds = new LinkedHashMap<String, Map<String, String>>();
    Map<String, String> variables = new LinkedHashMap<String, String>();
    variables.put("b", "2");
    variables.put("B", "1");
    Map<String, String> environment = new LinkedHashMap<String, String>();
    // Python orders by code point: the private-use character comes before the one outside the
    // basic plane, which `String.compareTo` would put first.
    environment.put("Z😀", "4");
    environment.put("Z", "3");
    environment.put("A", "1");
    environment.put("a", "2");
    kinds.put("variable", variables);
    kinds.put("environment", environment);
    String once = Settings.collect("ingest", kinds, on()).json();
    assertEquals(
        "{\"items\":["
            + "{\"kind\":\"environment\",\"name\":\"A\",\"value\":\"1\"},"
            + "{\"kind\":\"environment\",\"name\":\"Z\",\"value\":\"3\"},"
            + "{\"kind\":\"environment\",\"name\":\"Z😀\",\"value\":\"4\"},"
            + "{\"kind\":\"environment\",\"name\":\"a\",\"value\":\"2\"},"
            + "{\"kind\":\"variable\",\"name\":\"B\",\"value\":\"1\"},"
            + "{\"kind\":\"variable\",\"name\":\"b\",\"value\":\"2\"}]}",
        once);
    assertEquals(once, Settings.collect("ingest", kinds, on()).json());
  }

  @Test
  public void aNameOrValueThatNeedsEscapingIsStillOneJsonString() {
    Settings.Collected found =
        Settings.collect("ingest", kinds(env("QUOTE\"D", "say \"hi\"\n\\")), on());
    Map<String, Object> item = map(list(map(new Reader(found.json()).value()).get("items")).get(0));
    assertEquals("QUOTE\"D", item.get("name"));
    assertEquals("say \"hi\"\n\\", item.get("value"));
  }

  @Test
  public void whatCannotBeReadIsSentAsNothingAndNeverThrown() {
    Map<String, Map<String, String>> broken = new HashMap<String, Map<String, String>>();
    broken.put("environment", null);
    Settings.Collected found = Settings.collect("ingest", broken, on());
    assertTrue(found.isEmpty());
    assertTrue(found.excluded().isEmpty());
    assertTrue(Settings.collect("ingest", null, on()).isEmpty());
    // A key Python refuses to encode is refused here, and no setting is sent under it.
    assertTrue(Settings.collect("\ud800", kinds(env("DB_PASSWORD", "x")), on()).isEmpty());
  }

  @Test
  public void aNameWithNoValueIsNotASetting() {
    Map<String, String> held = env("TZ", "UTC");
    held.put("UNSET", null);
    assertEquals(
        "{\"items\":[{\"kind\":\"environment\",\"name\":\"TZ\",\"value\":\"UTC\"}]}",
        Settings.collect("ingest", kinds(held), on()).json());
  }

  private static Map<String, String> on(String... more) {
    Map<String, String> out = env(more);
    out.put("CONVALESCE_SEND_SETTINGS", "true");
    return out;
  }

  private static Map<String, String> env(String... pairs) {
    Map<String, String> out = new HashMap<String, String>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put(pairs[i], pairs[i + 1]);
    }
    return out;
  }

  private static Map<String, Map<String, String>> kinds(Map<String, String> environment) {
    return Collections.singletonMap(Settings.ENVIRONMENT, environment);
  }

  private static int count(String text, String part) {
    int found = 0;
    for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + 1)) {
      found++;
    }
    return found;
  }

  /** The shared vectors, found by walking up from wherever Gradle ran the tests. */
  private static Map<String, Object> vectors() throws IOException {
    File at = new File(System.getProperty("user.dir")).getAbsoluteFile();
    while (at != null && !new File(at, VECTORS).isFile()) {
      at = at.getParentFile();
    }
    assertTrue(VECTORS + " not found above " + System.getProperty("user.dir"), at != null);
    byte[] bytes = Files.readAllBytes(new File(at, VECTORS).toPath());
    return map(new Reader(new String(bytes, StandardCharsets.UTF_8)).value());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return (Map<String, Object>) value;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> list(Object value) {
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
