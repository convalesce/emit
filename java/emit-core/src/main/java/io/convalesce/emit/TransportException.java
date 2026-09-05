package io.convalesce.emit;

/**
 * An observation could not be delivered.
 *
 * <p>Caught before it reaches a customer's job; it exists so the transport can tell a refusal apart
 * from an outage.
 */
public final class TransportException extends Exception {

  private static final long serialVersionUID = 1L;

  private final int status;

  /**
   * @param message what went wrong
   * @param status the HTTP status, or 0 when the endpoint could not be reached at all
   */
  public TransportException(String message, int status) {
    super(message);
    this.status = status;
  }

  /**
   * @return the HTTP status, or 0 when there was none
   */
  public int status() {
    return status;
  }
}
