package io.convalesce.emit.spark;

import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

/**
 * Lets OpenLineage find {@link ConvalesceTransport} by the name {@code convalesce}.
 *
 * <p>OpenLineage looks transports up through {@link java.util.ServiceLoader}, and this jar lists
 * this class under {@code META-INF/services}. That listing is only read when OpenLineage asks for
 * it, so without openlineage-spark on the classpath nothing here is ever loaded and the listener
 * works as it always has.
 */
public final class ConvalesceTransportBuilder implements TransportBuilder {

  /** What {@code spark.openlineage.transport.type} names to choose this transport. */
  public static final String TYPE = "convalesce";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public TransportConfig getConfig() {
    return new ConvalesceTransportConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new ConvalesceTransport();
  }
}
