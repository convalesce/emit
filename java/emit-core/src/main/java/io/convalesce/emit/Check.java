package io.convalesce.emit;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;

/**
 * Checks this machine can reach Convalesce with its ingest key.
 *
 * <p>Run it on the Spark driver's host with the job's environment: {@code java -jar
 * convalesce-emit-core.jar}. It sends one empty batch, which the receiver accepts without writing
 * anything while recording that the key was used; that is what the console watches for to say it
 * heard from the tool. The same check as the Python client's {@code convalesce-emit check}.
 */
public final class Check {

  static final int CONNECTED = 0;
  static final int NOT_CONNECTED = 1;
  static final int MISCONFIGURED = 2;

  private static final byte[] EMPTY_BATCH =
      "{\"observations\":[]}".getBytes(Charset.forName("UTF-8"));
  private static final String NOT_A_KEY = "(not a Convalesce key)";

  private Check() {}

  /**
   * Runs the check against the process environment and exits with its answer.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    System.exit(run(Config.fromEnvironment(), System.out));
  }

  /**
   * Sends one empty batch and says what came of it.
   *
   * @param config what the job would send with
   * @param out where to write the answer
   * @return {@link #CONNECTED}, {@link #NOT_CONNECTED}, or {@link #MISCONFIGURED} when this
   *     configuration would send nothing at all
   */
  static int run(Config config, PrintStream out) {
    if (!config.enabled()) {
      out.println("Not checked: CONVALESCE_ENABLED is false, so this machine sends nothing.");
      return MISCONFIGURED;
    }
    if (config.dryRun()) {
      out.println(
          "Not checked: CONVALESCE_DRY_RUN is set, so this machine sends nothing. Unset it to"
              + " check.");
      return MISCONFIGURED;
    }
    String problem = config.validate();
    if (problem != null) {
      out.println("Not checked: " + problem);
      return MISCONFIGURED;
    }
    String url = Emitter.trimTrailingSlash(config.endpoint()) + Emitter.OBSERVATIONS_PATH;
    out.println("Checking " + url + " with key " + keyId(config.ingestKey()));
    try {
      Emitter.post(config, url, Emitter.gzip(EMPTY_BATCH));
    } catch (TransportException e) {
      out.println("Not connected: " + hint(e) + ".");
      return NOT_CONNECTED;
    } catch (IOException e) {
      out.println("Not checked: could not compress the batch: " + e.getMessage());
      return MISCONFIGURED;
    }
    out.println("Connected: the key was accepted, and Convalesce now shows it as heard from.");
    return CONNECTED;
  }

  /**
   * The public part of a key, safe to print.
   *
   * @param key the key as configured
   * @return {@code cvl_<scope>_<id>_...}, never the secret after it
   */
  static String keyId(String key) {
    if (key == null) {
      return NOT_A_KEY;
    }
    String[] parts = key.split("_", 4);
    if (parts.length == 4 && "cvl".equals(parts[0]) && !parts[2].isEmpty()) {
      return "cvl_" + parts[1] + "_" + parts[2] + "_...";
    }
    return NOT_A_KEY;
  }

  private static String hint(TransportException e) {
    switch (e.status()) {
      case 0:
        return e.getMessage()
            + ". Check the address, DNS, any proxy, and that this machine may reach it";
      case 401:
        return "the key was refused: it is mistyped, withdrawn, or not an ingest key for this"
            + " deployment";
      case 403:
        return "the key may not send observations: issue an ingest key for this tool";
      case 404:
        return "nothing answers at that path: for a self-hosted Convalesce, CONVALESCE_ENDPOINT"
            + " must end in /openapi";
      default:
        return "the endpoint answered HTTP " + e.status();
    }
  }
}
