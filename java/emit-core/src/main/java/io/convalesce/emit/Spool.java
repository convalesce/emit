package io.convalesce.emit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Local disk for batches that could not be delivered.
 *
 * <p>The same layout the Python client uses, so one host running both shares one queue: {@code
 * pending/} holds batches to send again after the next send that succeeds, {@code rejected/} holds
 * batches the receiver read and refused, kept rather than dropped. Each file is one gzip-compressed
 * request body, exactly as it would have gone on the wire.
 */
final class Spool {

  static final String PENDING = "pending";
  static final String REJECTED = "rejected";

  private static final Logger LOG = Logger.getLogger(Spool.class.getName());
  private static final String SUFFIX = ".json.gz";
  private static final String CLAIMED = ".sending";
  // A claim older than this belongs to a process that died mid-send.
  private static final long STALE_CLAIM_MS = 600_000L;

  private final File directory;
  private final long maxBytes;

  Spool(String directory, long maxBytes) {
    this.directory = new File(directory);
    this.maxBytes = maxBytes;
  }

  /**
   * Writes one compressed request body.
   *
   * @return the file written, or null when it could not be kept
   */
  File save(byte[] body, String kind) {
    File folder = new File(directory, kind);
    try {
      if (!folder.isDirectory() && !folder.mkdirs() && !folder.isDirectory()) {
        throw new IOException("cannot create " + folder);
      }
      if (PENDING.equals(kind) && size(folder) + body.length > maxBytes) {
        LOG.severe(
            "convalesce: spool "
                + folder
                + " is past "
                + maxBytes
                + " bytes; a batch of "
                + body.length
                + " bytes could not be kept");
        return null;
      }
      String name =
          String.format("%020d", System.currentTimeMillis() * 1_000_000L)
              + "-"
              + UUID.randomUUID().toString().replace("-", "")
              + SUFFIX;
      File path = new File(folder, name);
      File partial = new File(folder, name + ".partial");
      FileOutputStream out = new FileOutputStream(partial);
      try {
        out.write(body);
      } finally {
        out.close();
      }
      // Moved into place so a reader never sees half a file.
      Files.move(partial.toPath(), path.toPath(), StandardCopyOption.ATOMIC_MOVE);
      return path;
    } catch (IOException e) {
      LOG.severe("convalesce: could not spool a batch to " + folder + ": " + e.getMessage());
      return null;
    }
  }

  /** Every batch waiting to be sent again, oldest first, after releasing stale claims. */
  List<File> pending() {
    File[] files = new File(directory, PENDING).listFiles();
    List<File> out = new ArrayList<File>();
    if (files == null) {
      return out;
    }
    long now = System.currentTimeMillis();
    for (File file : files) {
      String name = file.getName();
      if (name.endsWith(SUFFIX)) {
        out.add(file);
      } else if (name.contains(CLAIMED) && now - file.lastModified() > STALE_CLAIM_MS) {
        File original = new File(file.getParentFile(), name.substring(0, name.indexOf(CLAIMED)));
        if (file.renameTo(original)) {
          out.add(original);
        }
      }
    }
    Collections.sort(out);
    return out;
  }

  /**
   * Takes one batch so no other process sends it at the same time.
   *
   * @return the claimed file, or null when another process got it first
   */
  File claim(File path) {
    File claimed = new File(path.getParentFile(), path.getName() + CLAIMED + "." + pid());
    if (!path.renameTo(claimed)) {
      return null;
    }
    claimed.setLastModified(System.currentTimeMillis());
    return claimed;
  }

  /** Puts a claimed batch back, to be tried again later. */
  void release(File claimed) {
    String name = claimed.getName();
    File original = new File(claimed.getParentFile(), name.substring(0, name.indexOf(CLAIMED)));
    if (!claimed.renameTo(original)) {
      LOG.severe("convalesce: could not return " + claimed + " to the spool");
    }
  }

  /** Moves a claimed batch the receiver refused to {@code rejected/}. */
  void reject(File claimed) {
    File folder = new File(directory, REJECTED);
    folder.mkdirs();
    String name = claimed.getName();
    if (!claimed.renameTo(new File(folder, name.substring(0, name.indexOf(CLAIMED))))) {
      LOG.severe("convalesce: could not keep refused batch " + claimed);
    }
  }

  static byte[] read(File claimed) throws IOException {
    return Files.readAllBytes(claimed.toPath());
  }

  static void done(File claimed) {
    if (!claimed.delete()) {
      LOG.warning("convalesce: could not remove delivered batch " + claimed);
    }
  }

  private static long size(File folder) {
    long total = 0;
    File[] files = folder.listFiles();
    if (files != null) {
      for (File file : files) {
        total += file.length();
      }
    }
    return total;
  }

  private static String pid() {
    // "pid@host" on every JVM this targets; Java 8 has no ProcessHandle.
    String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    int at = name.indexOf('@');
    return at > 0 ? name.substring(0, at) : name;
  }
}
