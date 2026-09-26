package io.convalesce.emit.spark;

import io.openlineage.client.transports.TransportConfig;

/**
 * Deliberately empty.
 *
 * <p>The key, endpoint and limits come from the same {@code CONVALESCE_*} variables the listener
 * reads, so there is one place to configure a driver and the two cannot disagree. OpenLineage still
 * needs a config class to deserialise {@code spark.openlineage.transport.*} into.
 */
public final class ConvalesceTransportConfig implements TransportConfig {}
